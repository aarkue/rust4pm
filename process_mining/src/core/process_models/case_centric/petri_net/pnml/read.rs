//! Reading Petri nets from PNML
use std::{
    collections::{HashMap, HashSet},
    io::BufRead,
};

use quick_xml::{events::BytesStart, Error as QuickXMLError, Reader};

use crate::core::{
    io::{push_xml_general_ref, read_xml_text_unescaped},
    process_models::case_centric::petri_net::{
        io::PetriNetIOError,
        marking::Marking,
        net::{AcceptingPetriNet, ArcType, PetriNet, PlaceID, TransitionID},
    },
};

/// Error in the content of PNML data. IO and XML errors are other [`PetriNetIOError`] variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PNMLParseError {
    /// Missing key on XML element (with expected key included)
    MissingKey(&'static str),
    /// Encountered no PNML tag (i.e., the parsed data was not a PNML file)
    NoPNMLTag,
    /// No place has an initial marking, but one was required
    MissingInitialMarking,
    /// The file has no final marking, but one was required
    MissingFinalMarking,
    /// The file has more than one final marking (with their count)
    MultipleFinalMarkings(usize),
    /// A token count or arc weight is not a number in `0..=u32::MAX` (with the text)
    InvalidTokenCount(String),
    /// Token counts of a place, or weights of arcs between the same nodes, sum up to more than
    /// `u32::MAX` (with the PNML id of the place, or of the arc's source)
    TokenCountOverflow(String),
    /// An arc or a final marking refers to a node that is not in the net (with its id)
    UnknownNode(String),
    /// Two nodes share an id (with the id)
    DuplicateNode(String),
    /// An arc connects two places or two transitions
    InvalidArc {
        /// Id of the arc's source
        source: String,
        /// Id of the arc's target
        target: String,
    },
    /// The file has more than one `<net>`
    MultipleNets,
    /// An arc has an `<arctype>` other than `normal`, e.g., `inhibitor` or `reset` (with the type)
    UnsupportedArcType(String),
    /// A `<referencePlace>` or `<referenceTransition>` refers to a node of the other kind or is
    /// part of a reference cycle (with its id)
    InvalidReference(String),
    /// The net is not a place/transition net, e.g., a symmetric or high-level net (with its
    /// `type`, or the high-level element found, e.g., `hlinscription`)
    UnsupportedNetType(String),
}

impl std::fmt::Display for PNMLParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to parse PNML: {self:?}")
    }
}

impl std::error::Error for PNMLParseError {}

const IN_TRANSITION: &str = "a transition is read in this mode";
const IN_PLACE: &str = "a place is read in this mode";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    None,
    Net,
    Place,
    PlaceName,
    InitialMarking,
    Transition,
    TransitionName,
    Arc,
    ArcInscription,
    ArcType,
    FinalMarkings,
    FinalMarking,
    FinalMarkingPlace,
}

/// A place as read, added to the net once its name is known
struct PlaceInfo {
    pnml_id: String,
    name_text: Option<String>,
    /// The tokens of its `<initialMarking>`, if it has one
    tokens: Option<u32>,
}

/// A transition as read, added to the net once its label is known
struct TransitionInfo {
    pnml_id: String,
    name_text: Option<String>,
    /// The event name in `ProM` 5.2's `<logevent>`, whose `<name>` text also has the event type
    logevent_name: Option<String>,
    silent: bool,
}

/// Path of `ProM` 5.2's event name inside a transition's `<toolspecific>`
const LOGEVENT_NAME: [&str; 2] = ["logevent", "name"];

#[derive(Clone, Copy)]
enum Node {
    Place(PlaceID),
    Transition(TransitionID),
}

fn attribute(element: &BytesStart<'_>, key: &'static str) -> Result<String, PetriNetIOError> {
    let value = element
        .try_get_attribute(key)
        .map_err(QuickXMLError::from)?
        .ok_or(PNMLParseError::MissingKey(key))?
        .value;
    Ok(read_xml_text_unescaped(&value))
}

/// Whether `net_type` is the PNML type of place/transition nets, or one of the variants `ProM`,
/// pm4py and other tools write for them
fn is_pt_net_type(net_type: &str) -> bool {
    let net_type = net_type.trim().trim_end_matches('/');
    let name = net_type.rsplit('/').next().unwrap_or(net_type);
    let name = name.split_once('.').map_or(name, |(name, _)| name);
    net_type.is_empty()
        || net_type == "P/T net"
        || ["ptnet", "pnmlcoremodel", "basicPNML", "ptNetb"]
            .iter()
            .any(|known| name.eq_ignore_ascii_case(known))
}

fn token_count(text: String) -> Result<u32, PNMLParseError> {
    text.parse()
        .map_err(|_| PNMLParseError::InvalidTokenCount(text))
}

/// Import a Petri net from PNML
///
/// Returns the net, its initial marking (`None` if no place has an `<initialMarking>`, which may
/// also hold 0 tokens) and its final marking (`None` without `ProM`'s `<finalmarkings>`
/// extension). Importing an [`AcceptingPetriNet`] through [`Importable`](crate::Importable)
/// requires both markings, and importing a [`PetriNet`] keeps the structure alone.
///
/// Places and transitions get ids in file order. A place's name is the text of its `<name>`, or
/// its PNML id if it has none, and a transition's name is its PNML id. Arcs, markings and
/// references name nodes by PNML id. A transition is silent if `ProM`'s `toolspecific` tag marks
/// it invisible; otherwise its label is the event name in `ProM` 5.2's `<logevent>`, the text of
/// its `<name>`, or its PNML id if it has neither. Arcs between the same nodes merge. Pages are flattened, reference nodes stand for the
/// node they refer to, and other `<toolspecific>` content is ignored.
///
/// Nets other than place/transition nets (by the `type` of `<net>`, or by high-level elements such
/// as `<hlinscription>`), several nets in one file, inhibitor, reset and other special arcs
/// (`<arctype>`), and several final markings are not supported and are an error.
pub fn import_pnml(
    reader: impl BufRead,
) -> Result<(PetriNet, Option<Marking>, Option<Marking>), PetriNetIOError> {
    let mut reader = Reader::from_reader(reader);
    // Text is trimmed below instead, as references split it into several events, each of which
    // the reader would trim
    reader.config_mut().trim_text(false);
    // A lone `&` in text is kept as is, like an unknown entity
    reader.config_mut().allow_dangling_amp = true;
    reader.config_mut().expand_empty_elements = true;
    let mut buf = Vec::new();

    let mut mode = Mode::None;
    let mut encountered_pnml_tag = false;
    let mut net = PetriNet::new();
    let mut transitions: Vec<TransitionInfo> = Vec::new();
    let mut place_ids: HashMap<String, PlaceID> = HashMap::new();
    let mut arcs: Vec<(String, String, u32)> = Vec::new();
    let mut initial_marking = Marking::new();
    let mut has_initial_marking = false;
    let mut final_markings: Vec<Vec<(String, u32)>> = Vec::new();
    let mut current_place: Option<PlaceInfo> = None;
    let mut current_ref: Option<String> = None;
    let mut text_run = String::new();
    let mut seen_net = false;
    // Reference nodes as (id, referenced id, whether it is a `referencePlace`)
    let mut references: Vec<(String, String, bool)> = Vec::new();
    // Depth inside a `<toolspecific>`, whose content is skipped except for `LOGEVENT_NAME`
    let mut tool_depth = 0usize;
    // Whether that `<toolspecific>` is ProM's on a transition
    let mut prom_transition_tool = false;
    // How many elements of `LOGEVENT_NAME` the reader is in
    let mut logevent_depth = 0usize;

    loop {
        let event = reader.read_event_into(&mut buf)?;
        if tool_depth > 0 {
            let in_logevent_name = logevent_depth == LOGEVENT_NAME.len() && tool_depth == 3;
            match event {
                quick_xml::events::Event::Start(b) => {
                    tool_depth += 1;
                    if prom_transition_tool
                        && logevent_depth + 2 == tool_depth
                        && LOGEVENT_NAME.get(logevent_depth) == Some(&b.local_name().as_ref())
                    {
                        logevent_depth += 1;
                    }
                }
                quick_xml::events::Event::End(_) => {
                    if logevent_depth > 0 && logevent_depth + 1 == tool_depth {
                        if in_logevent_name {
                            let text = text_run.trim_matches([' ', '\t', '\r', '\n']);
                            let transition = transitions.last_mut().expect(IN_TRANSITION);
                            if !text.is_empty() && transition.logevent_name.is_none() {
                                transition.logevent_name = Some(text.to_string());
                            }
                            text_run.clear();
                        }
                        logevent_depth -= 1;
                    }
                    tool_depth -= 1;
                }
                quick_xml::events::Event::Text(t) if in_logevent_name => text_run.push_str(&t),
                quick_xml::events::Event::CData(t) if in_logevent_name => text_run.push_str(&t),
                quick_xml::events::Event::GeneralRef(r) if in_logevent_name => {
                    push_xml_general_ref(&mut text_run, &r);
                }
                quick_xml::events::Event::Eof => break,
                _ => {}
            }
            buf.clear();
            continue;
        }
        if !matches!(
            event,
            quick_xml::events::Event::Text(_)
                | quick_xml::events::Event::GeneralRef(_)
                | quick_xml::events::Event::CData(_)
        ) {
            let text = text_run.trim_matches([' ', '\t', '\r', '\n']).to_string();
            text_run.clear();
            if !text.is_empty() {
                match mode {
                    Mode::TransitionName => {
                        transitions.last_mut().expect(IN_TRANSITION).name_text = Some(text);
                    }
                    Mode::InitialMarking => {
                        let place = current_place.as_mut().expect(IN_PLACE);
                        let tokens = place.tokens.unwrap_or(0).checked_add(token_count(text)?);
                        let Some(tokens) = tokens else {
                            return Err(
                                PNMLParseError::TokenCountOverflow(place.pnml_id.clone()).into()
                            );
                        };
                        place.tokens = Some(tokens);
                    }
                    Mode::PlaceName => {
                        current_place.as_mut().expect(IN_PLACE).name_text = Some(text);
                    }
                    Mode::FinalMarkingPlace => {
                        let marking = final_markings.last_mut().expect("a marking is read");
                        // Only the first text of a place reference counts
                        if let Some(place) = current_ref.take() {
                            marking.push((place, token_count(text)?));
                        }
                    }
                    Mode::ArcInscription => {
                        arcs.last_mut().expect("an arc is read in this mode").2 =
                            token_count(text)?;
                    }
                    Mode::ArcType if text != "normal" => {
                        return Err(PNMLParseError::UnsupportedArcType(text).into());
                    }
                    _ => {}
                }
            }
        }
        match event {
            quick_xml::events::Event::Start(b) => match (b.local_name().as_ref(), mode) {
                ("pnml", _) => encountered_pnml_tag = true,
                ("net", _) if seen_net => return Err(PNMLParseError::MultipleNets.into()),
                ("net", _) => {
                    let net_type = b.try_get_attribute("type").map_err(QuickXMLError::from)?;
                    if let Some(net_type) = net_type.map(|t| read_xml_text_unescaped(&t.value)) {
                        if !is_pt_net_type(&net_type) {
                            return Err(PNMLParseError::UnsupportedNetType(net_type).into());
                        }
                    }
                    seen_net = true;
                    mode = Mode::Net;
                }
                (element @ ("hlinitialMarking" | "hlinscription"), _) => {
                    return Err(PNMLParseError::UnsupportedNetType(element.to_string()).into());
                }
                ("page", _) => mode = Mode::Net,
                (kind @ ("referencePlace" | "referenceTransition"), _) => references.push((
                    attribute(&b, "id")?,
                    attribute(&b, "ref")?,
                    kind == "referencePlace",
                )),
                ("place", Mode::FinalMarking) => {
                    current_ref = Some(attribute(&b, "idref")?);
                    mode = Mode::FinalMarkingPlace;
                }
                ("place", _) => {
                    current_place = Some(PlaceInfo {
                        pnml_id: attribute(&b, "id")?,
                        name_text: None,
                        tokens: None,
                    });
                    mode = Mode::Place;
                }
                ("transition", _) => {
                    transitions.push(TransitionInfo {
                        pnml_id: attribute(&b, "id")?,
                        name_text: None,
                        logevent_name: None,
                        silent: false,
                    });
                    mode = Mode::Transition;
                }
                ("arc", _) => {
                    arcs.push((attribute(&b, "source")?, attribute(&b, "target")?, 1));
                    mode = Mode::Arc;
                }
                ("inscription", Mode::Arc) => mode = Mode::ArcInscription,
                ("arctype", Mode::Arc) => mode = Mode::ArcType,
                ("toolspecific", _) => {
                    tool_depth = 1;
                    let attr = b
                        .try_get_attribute("activity")
                        .map_err(QuickXMLError::from)?;
                    if mode == Mode::Transition
                        && attr.is_some_and(|attr| attr.value.as_ref() == "$invisible$")
                    {
                        transitions.last_mut().expect(IN_TRANSITION).silent = true;
                    }
                    let tool = b.try_get_attribute("tool").map_err(QuickXMLError::from)?;
                    prom_transition_tool = mode == Mode::Transition
                        && tool.is_some_and(|tool| tool.value.as_ref() == "ProM");
                }
                ("initialMarking", Mode::Place) => {
                    has_initial_marking = true;
                    current_place
                        .as_mut()
                        .expect(IN_PLACE)
                        .tokens
                        .get_or_insert(0);
                    mode = Mode::InitialMarking;
                }
                ("finalmarkings", _) => mode = Mode::FinalMarkings,
                ("marking", Mode::FinalMarkings) => {
                    final_markings.push(Vec::new());
                    mode = Mode::FinalMarking;
                }
                ("name", Mode::Place) => mode = Mode::PlaceName,
                ("name", Mode::Transition) => mode = Mode::TransitionName,
                _ => {}
            },
            quick_xml::events::Event::End(b) => match (b.local_name().as_ref(), mode) {
                ("place", Mode::FinalMarkingPlace) => mode = Mode::FinalMarking,
                ("place", _) => {
                    if let Some(place) = current_place.take() {
                        let name = place.name_text.unwrap_or_else(|| place.pnml_id.clone());
                        let id = net.add_place(Some(name));
                        if place_ids.insert(place.pnml_id.clone(), id).is_some() {
                            return Err(PNMLParseError::DuplicateNode(place.pnml_id).into());
                        }
                        if let Some(tokens) = place.tokens {
                            initial_marking.set(id, tokens);
                        }
                    }
                    mode = Mode::Net;
                }
                ("transition" | "arc", _) => mode = Mode::Net,
                ("initialMarking", Mode::InitialMarking) => mode = Mode::Place,
                ("inscription", Mode::ArcInscription) => mode = Mode::Arc,
                ("arctype", Mode::ArcType) => mode = Mode::Arc,
                ("marking", Mode::FinalMarking) => mode = Mode::FinalMarkings,
                ("finalmarkings", _) => mode = Mode::Net,
                ("name", Mode::PlaceName) => mode = Mode::Place,
                ("name", Mode::TransitionName) => mode = Mode::Transition,
                _ => {}
            },
            quick_xml::events::Event::Text(t) => text_run.push_str(&t),
            quick_xml::events::Event::CData(t) => text_run.push_str(&t),
            quick_xml::events::Event::GeneralRef(r) => push_xml_general_ref(&mut text_run, &r),
            quick_xml::events::Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if !encountered_pnml_tag {
        return Err(PNMLParseError::NoPNMLTag.into());
    }

    let mut nodes: HashMap<String, Node> = place_ids
        .iter()
        .map(|(pnml_id, place)| (pnml_id.clone(), Node::Place(*place)))
        .collect();
    for t in transitions {
        let label = (!t.silent).then(|| {
            t.logevent_name
                .or(t.name_text)
                .unwrap_or_else(|| t.pnml_id.clone())
        });
        let id = net.add_transition(label, Some(t.pnml_id.clone()));
        if nodes
            .insert(t.pnml_id.clone(), Node::Transition(id))
            .is_some()
        {
            return Err(PNMLParseError::DuplicateNode(t.pnml_id).into());
        }
    }
    let targets: HashMap<&str, &str> = references
        .iter()
        .map(|(id, target, _)| (id.as_str(), target.as_str()))
        .collect();
    // Nodes of the references passed through by earlier chains, so each chain link is walked once
    let mut via: HashMap<&str, Node> = HashMap::new();
    let mut chain: HashSet<&str> = HashSet::new();
    let mut resolved = Vec::with_capacity(references.len());
    for (id, target, is_place) in &references {
        let mut target = target.as_str();
        let node = loop {
            if let Some(node) = nodes.get(target).or_else(|| via.get(target)) {
                break *node;
            }
            let Some(next) = targets.get(target) else {
                return Err(PNMLParseError::UnknownNode(target.to_string()).into());
            };
            if !chain.insert(target) {
                return Err(PNMLParseError::InvalidReference(id.clone()).into());
            }
            target = next;
        };
        via.extend(chain.drain().map(|t| (t, node)));
        if matches!(node, Node::Place(_)) != *is_place {
            return Err(PNMLParseError::InvalidReference(id.clone()).into());
        }
        resolved.push((id.clone(), node));
    }
    for (id, node) in resolved {
        if nodes.insert(id.clone(), node).is_some() {
            return Err(PNMLParseError::DuplicateNode(id).into());
        }
    }
    let node = |id: &String| {
        nodes
            .get(id)
            .copied()
            .ok_or_else(|| PNMLParseError::UnknownNode(id.clone()))
    };
    for (source, target, weight) in arcs {
        let from_to = match (node(&source)?, node(&target)?) {
            (Node::Place(p), Node::Transition(t)) => ArcType::PlaceTransition(p, t),
            (Node::Transition(t), Node::Place(p)) => ArcType::TransitionPlace(t, p),
            _ => return Err(PNMLParseError::InvalidArc { source, target }.into()),
        };
        if net.checked_add_arc(from_to, Some(weight)).is_none() {
            return Err(PNMLParseError::TokenCountOverflow(source).into());
        }
    }

    let initial_marking = has_initial_marking.then_some(initial_marking);
    if final_markings.len() > 1 {
        return Err(PNMLParseError::MultipleFinalMarkings(final_markings.len()).into());
    }
    let final_marking = final_markings
        .pop()
        .map(|marking| {
            let mut final_marking = Marking::new();
            for (pnml_id, tokens) in marking {
                let Some(Node::Place(place)) = nodes.get(&pnml_id) else {
                    return Err(PNMLParseError::UnknownNode(pnml_id));
                };
                if final_marking.checked_add(*place, tokens).is_none() {
                    return Err(PNMLParseError::TokenCountOverflow(pnml_id));
                }
            }
            Ok(final_marking)
        })
        .transpose()?;
    Ok((net, initial_marking, final_marking))
}

/// The accepting net of what [`import_pnml`] read, if both markings are present
///
/// A net without places has nowhere to write an initial marking, so it gets the empty one.
pub(crate) fn accepting_net(
    (net, initial_marking, final_marking): (PetriNet, Option<Marking>, Option<Marking>),
) -> Result<AcceptingPetriNet, PNMLParseError> {
    let initial_marking = initial_marking.or_else(|| (net.num_places() == 0).then(Marking::new));
    Ok(AcceptingPetriNet::new(
        net,
        initial_marking.ok_or(PNMLParseError::MissingInitialMarking)?,
        final_marking.ok_or(PNMLParseError::MissingFinalMarking)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::{import_pnml, PNMLParseError};
    use crate::{
        core::process_models::case_centric::petri_net::{
            io::PetriNetIOError,
            marking::Marking,
            net::{PetriNet, PlaceID, TransitionID},
        },
        test_utils::get_test_data_path,
    };

    const SMALL: &str = r#"<pnml><net id="net1" type="http://www.pnml.org/version-2009/grammar/pnmlcoremodel"><page id="page1">
  <place id="src"><initialMarking><text>1</text></initialMarking></place>
  <place id="sink"/>
  <transition id="t_b"><name><text>b</text></name></transition>
  <transition id="t_tau1"><toolspecific tool="ProM" version="6.4" activity="$invisible$"/><name><text>tau</text></name></transition>
  <transition id="t_tau2"><name><text>tau</text></name><toolspecific tool="ProM" version="6.4" activity="$invisible$"/></transition>
  <transition id="t_noname"/>
  <arc id="a1" source="src" target="t_b"><inscription><text>2</text></inscription></arc>
  <arc id="a2" source="src" target="t_b"/>
  <arc id="a3" source="t_b" target="sink"/>
</page><finalmarkings><marking><place idref="sink"><text>1</text></place><place idref="src"><text>0</text></place></marking></finalmarkings></net></pnml>"#;

    fn petri_net_file(name: &str) -> std::path::PathBuf {
        get_test_data_path().join("petri-net").join(name)
    }

    fn import_file(
        path: impl AsRef<std::path::Path>,
    ) -> Result<(PetriNet, Option<Marking>, Option<Marking>), PetriNetIOError> {
        import_pnml(std::io::BufReader::new(std::fs::File::open(path)?))
    }

    #[test]
    fn reads_labels_silent_flags_and_file_order() {
        let (net, initial, final_marking) = import_pnml(SMALL.as_bytes()).unwrap();
        let labels: Vec<_> = net.transitions().map(|(_, t)| t.label.clone()).collect();
        assert_eq!(
            labels,
            vec![Some("b".into()), None, None, Some("t_noname".into())]
        );
        let names: Vec<_> = net.places().map(|(_, p)| p.name.clone().unwrap()).collect();
        assert_eq!(names, vec!["src", "sink"]);
        let (src, sink, b) = (PlaceID(0), PlaceID(1), TransitionID(0));
        assert_eq!(net.transition(b).inputs(), &[(src, 3)]);
        assert_eq!(initial, Some(Marking::from([(src, 1)])));
        assert_eq!(final_marking, Some(Marking::from([(sink, 1)])));
    }

    #[test]
    fn multiple_final_markings_are_an_error() {
        let pnml = SMALL.replace(
            "</marking></finalmarkings>",
            "</marking><marking/></finalmarkings>",
        );
        assert!(matches!(
            import_pnml(pnml.as_bytes()),
            Err(PetriNetIOError::Pnml(
                PNMLParseError::MultipleFinalMarkings(2)
            ))
        ));
    }

    #[test]
    fn references_to_unknown_nodes_are_an_error() {
        let arc = SMALL.replace(r#"target="sink"/>"#, r#"target="nowhere"/>"#);
        assert!(matches!(
            import_pnml(arc.as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::UnknownNode(id))) if id == "nowhere"
        ));
        let marking = SMALL.replace(r#"<place idref="sink">"#, r#"<place idref="nowhere">"#);
        assert!(matches!(
            import_pnml(marking.as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::UnknownNode(id))) if id == "nowhere"
        ));
    }

    #[test]
    fn duplicate_ids_are_an_error() {
        let places = SMALL.replace(r#"<place id="sink"/>"#, r#"<place id="src"/>"#);
        assert!(matches!(
            import_pnml(places.as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::DuplicateNode(id))) if id == "src"
        ));
        let mixed = SMALL.replace(
            r#"<transition id="t_noname"/>"#,
            r#"<transition id="sink"/>"#,
        );
        assert!(matches!(
            import_pnml(mixed.as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::DuplicateNode(id))) if id == "sink"
        ));
    }

    #[test]
    fn arcs_between_places_are_an_error() {
        let pnml = SMALL.replace(
            r#"source="t_b" target="sink""#,
            r#"source="src" target="sink""#,
        );
        assert!(matches!(
            import_pnml(pnml.as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::InvalidArc { .. }))
        ));
    }

    #[test]
    fn overflowing_sums_are_an_error() {
        let max = u32::MAX;
        let arcs = SMALL.replace(
            "<inscription><text>2</text>",
            &format!("<inscription><text>{max}</text>"),
        );
        assert!(matches!(
            import_pnml(arcs.as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::TokenCountOverflow(id))) if id == "src"
        ));
        let initial = SMALL.replace(
            "<initialMarking><text>1</text>",
            &format!("<initialMarking><text>{max}</text><text>1</text>"),
        );
        assert!(matches!(
            import_pnml(initial.as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::TokenCountOverflow(id))) if id == "src"
        ));
        let final_marking = SMALL.replace(
            r#"<place idref="src"><text>0</text></place>"#,
            &format!(r#"<place idref="sink"><text>{max}</text></place>"#),
        );
        assert!(matches!(
            import_pnml(final_marking.as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::TokenCountOverflow(id))) if id == "sink"
        ));
    }

    #[test]
    fn invalid_token_count_is_an_error() {
        let pnml = SMALL.replace(
            "<initialMarking><text>1</text>",
            "<initialMarking><text>abc</text>",
        );
        assert!(matches!(
            import_pnml(pnml.as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::InvalidTokenCount(_)))
        ));
    }

    #[test]
    fn toolspecific_content_is_ignored() {
        let pnml = SMALL
            .replace(
                r#"<name><text>b</text></name>"#,
                r#"<name><text>b+start</text></name><toolspecific tool="x" version="5.2"><logevent><name>b</name><type>start</type></logevent></toolspecific>"#,
            )
            .replace(
                "<text>1</text></initialMarking>",
                r#"<text>1</text><toolspecific tool="x" version="1"><text>5</text></toolspecific></initialMarking>"#,
            )
            .replace(
                "</page>",
                r#"</page><toolspecific tool="x" version="1"><place id="src"/><transition id="extra"/><net id="n2"/></toolspecific>"#,
            );
        let (net, initial, _) = import_pnml(pnml.as_bytes()).unwrap();
        assert_eq!(
            net.transition(TransitionID(0)).label.as_deref(),
            Some("b+start")
        );
        assert_eq!(net.num_transitions(), 4);
        assert_eq!(initial, Some(Marking::from([(PlaceID(0), 1)])));
    }

    #[test]
    fn prom_5_2_event_names_are_labels() {
        let label = |transition: &str| {
            let pnml = SMALL.replace(
                r#"<transition id="t_b"><name><text>b</text></name></transition>"#,
                transition,
            );
            let (net, _, _) = import_pnml(pnml.as_bytes()).unwrap();
            net.transition(TransitionID(0)).label.clone()
        };
        let logevent = |name: &str| {
            format!(
                r#"<toolspecific tool="ProM" version="5.2"><graphics><name>g</name></graphics><logevent><name>{name}</name><type>complete</type></logevent></toolspecific>"#
            )
        };
        assert_eq!(
            label(&format!(
                r#"<transition id="t_b"><name><text>b\ncomplete</text></name>{}</transition>"#,
                logevent(" b &amp; c\n")
            )),
            Some("b & c".into())
        );
        assert_eq!(
            label(&format!(
                r#"<transition id="t_b">{}<name><text>b+complete</text></name></transition>"#,
                logevent("<![CDATA[b]]>")
            )),
            Some("b".into())
        );
        assert_eq!(
            label(&format!(
                r#"<transition id="t_b"><name><text>b</text></name>{}</transition>"#,
                logevent("")
            )),
            Some("b".into())
        );
    }

    #[test]
    fn non_pt_nets_are_an_error() {
        let with_type = |net_type: &str| {
            SMALL.replace(
                r#"type="http://www.pnml.org/version-2009/grammar/pnmlcoremodel""#,
                net_type,
            )
        };
        for net_type in [
            r#"type="http://www.pnml.org/version-2009/grammar/ptnet""#,
            r#"type="http://www.pnml.org/version-2009/grammar/ptnet/""#,
            r#"type="http://www.pnml.org/grammar/ptnet""#,
            r#"type="http://www.pnml.org/version-2009/grammar/pnmlcoremodel/""#,
            r#"type="http://www.informatik.hu-berlin.de/top/pnml/basicPNML.rng""#,
            r#"type="http://www.informatik.hu-berlin.de/top/pntd/ptNetb""#,
            r#"type="P/T net""#,
            "",
        ] {
            assert!(
                import_pnml(with_type(net_type).as_bytes()).is_ok(),
                "{net_type}"
            );
        }
        for net_type in ["symmetricnet", "highlevelnet", "pt-hlpng"] {
            let pnml = with_type(&format!(
                r#"type="http://www.pnml.org/version-2009/grammar/{net_type}""#
            ));
            assert!(matches!(
                import_pnml(pnml.as_bytes()),
                Err(PetriNetIOError::Pnml(PNMLParseError::UnsupportedNetType(t))) if t.ends_with(net_type)
            ));
        }
        let hl_marking = SMALL.replace(
            "<initialMarking><text>1</text></initialMarking>",
            "<hlinitialMarking><text>1'dot</text></hlinitialMarking>",
        );
        let hl_arc = SMALL.replace(
            r#"target="sink"/>"#,
            r#"target="sink"><hlinscription><text>x</text></hlinscription></arc>"#,
        );
        for (pnml, element) in [(hl_marking, "hlinitialMarking"), (hl_arc, "hlinscription")] {
            assert!(matches!(
                import_pnml(pnml.as_bytes()),
                Err(PetriNetIOError::Pnml(PNMLParseError::UnsupportedNetType(e))) if e == element
            ));
        }
    }

    #[test]
    fn cdata_and_prefixed_elements_are_read() {
        let pnml = SMALL
            .replace("<text>b</text>", "<text><![CDATA[b<1>]]></text>")
            .replace(
                "<pnml>",
                r#"<p:pnml xmlns:p="http://www.pnml.org/version-2009/grammar/pnml">"#,
            )
            .replace("</pnml>", "</p:pnml>")
            .replace("<place ", "<p:place ")
            .replace("</place>", "</p:place>");
        let (net, initial, final_marking) = import_pnml(pnml.as_bytes()).unwrap();
        assert_eq!(
            net.transition(TransitionID(0)).label.as_deref(),
            Some("b<1>")
        );
        assert_eq!(initial, Some(Marking::from([(PlaceID(0), 1)])));
        assert_eq!(final_marking, Some(Marking::from([(PlaceID(1), 1)])));
    }

    #[test]
    fn special_arcs_are_an_error() {
        let arc = |kind: &str| {
            SMALL.replace(
                r#"target="sink"/>"#,
                &format!(r#"target="sink"><arctype><text>{kind}</text></arctype></arc>"#),
            )
        };
        assert!(import_pnml(arc("normal").as_bytes()).is_ok());
        assert!(matches!(
            import_pnml(arc("inhibitor").as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::UnsupportedArcType(kind))) if kind == "inhibitor"
        ));
    }

    #[test]
    fn several_nets_are_an_error() {
        let net = &SMALL[SMALL.find("<net ").unwrap()..SMALL.find("</pnml>").unwrap()];
        let pnml = SMALL.replace(
            "</pnml>",
            &format!("{}</pnml>", net.replace("net1", "net2")),
        );
        assert!(matches!(
            import_pnml(pnml.as_bytes()),
            Err(PetriNetIOError::Pnml(PNMLParseError::MultipleNets))
        ));
    }

    #[test]
    fn reference_nodes_stand_for_their_nodes() {
        let with = |nodes: &str| {
            SMALL.replace(
                "</page>",
                &format!(r#"<page id="page2"><page id="page3">{nodes}<arc id="a4" source="rt" target="rp"/></page></page></page>"#),
            )
        };
        let pnml = with(
            r#"<referencePlace id="rp0" ref="sink"/><referencePlace id="rp" ref="rp0"/><referenceTransition id="rt" ref="t_b"/>"#,
        );
        let (net, _, _) = import_pnml(pnml.as_bytes()).unwrap();
        assert_eq!(net.num_places(), 2);
        assert_eq!(
            net.transition(TransitionID(0)).outputs(),
            &[(PlaceID(1), 2)]
        );

        let wrong_kind =
            with(r#"<referencePlace id="rp" ref="sink"/><referenceTransition id="rt" ref="src"/>"#);
        let cycle = with(
            r#"<referencePlace id="rp" ref="rp2"/><referencePlace id="rp2" ref="rp"/><referenceTransition id="rt" ref="t_b"/>"#,
        );
        for pnml in [wrong_kind, cycle] {
            assert!(matches!(
                import_pnml(pnml.as_bytes()),
                Err(PetriNetIOError::Pnml(PNMLParseError::InvalidReference(_)))
            ));
        }
    }

    #[test]
    fn long_reference_chains_resolve() {
        let n = 20_000;
        let refs = |order: &mut dyn Iterator<Item = usize>| {
            let refs: String = order
                .map(|i| match i {
                    0 => r#"<referencePlace id="r0" ref="sink"/>"#.to_string(),
                    i => format!(r#"<referencePlace id="r{i}" ref="r{}"/>"#, i - 1),
                })
                .collect();
            SMALL.replace(
                "</page>",
                &format!(
                    r#"{refs}<arc id="a4" source="t_b" target="r{}"/></page>"#,
                    n - 1
                ),
            )
        };
        for pnml in [refs(&mut (0..n).rev()), refs(&mut (0..n))] {
            let (net, _, _) = import_pnml(pnml.as_bytes()).unwrap();
            assert_eq!(net.num_places(), 2);
            assert_eq!(
                net.transition(TransitionID(0)).outputs(),
                &[(PlaceID(1), 2)]
            );
        }
    }

    #[test]
    fn pnml_import() {
        let (net, initial, final_marking) = import_file(petri_net_file("pn.pnml")).unwrap();
        assert_eq!(net.num_transitions(), 46);
        assert_eq!(net.num_places(), 9);
        assert_eq!(net.arcs().count(), 24);
        assert!(initial.is_some());
        assert!(final_marking.is_some());
        assert!(net.arcs().any(|arc| arc.weight == 1337));
    }

    #[test]
    fn nets_without_markings_import() {
        let (net, initial, final_marking) =
            import_file(petri_net_file("bpic12-tsinghua.pnml")).unwrap();
        assert_eq!(net.num_transitions(), 10);
        assert_eq!((initial, final_marking), (None, None));
        // ProM 5.2 appends a literal `\n` and the event type to `<name>`
        let labels: Vec<_> = net
            .transitions()
            .map(|(_, t)| t.label.clone().unwrap())
            .collect();
        assert!(labels.contains(&"W_Beoordelen fraude".to_string()));
        assert!(labels.iter().all(|label| !label.contains('\\')));
    }

    #[test]
    fn import_is_deterministic() {
        let path = petri_net_file("sepsis-DISCovered.apnml");
        assert_eq!(import_file(&path).unwrap(), import_file(&path).unwrap());
    }

    #[test]
    fn not_a_pnml_file_is_an_error() {
        assert!(import_file(petri_net_file("not-a-petri-net.slang")).is_err());
    }
}
