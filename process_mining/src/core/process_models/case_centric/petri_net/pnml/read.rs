//! Reading Petri nets from PNML
use std::{collections::HashMap, io::BufRead};

use quick_xml::{events::BytesStart, Error as QuickXMLError, Reader};

use crate::core::{
    io::read_xml_text_unescaped,
    process_models::case_centric::petri_net::{
        marking::Marking,
        net::{AcceptingPetriNet, ArcType, PetriNet, PlaceID, TransitionID},
    },
};

///
/// Error encountered while parsing PNML
///
#[derive(Debug, Clone)]
pub enum PNMLParseError {
    /// IO error
    IOError(std::sync::Arc<std::io::Error>),
    /// XML error (e.g., incorrect XML format )
    XMLParsingError(QuickXMLError),
    /// Missing key on XML element (with expected key included)
    MissingKey(&'static str),
    /// Encountered no PNML tag (i.e., the parsed data was not a PNML file)
    NoPNMLTag,
    /// No place is marked initially, but an initial marking was required
    MissingInitialMarking,
    /// The file has no final marking, but one was required
    MissingFinalMarking,
    /// The file has more than one final marking (with their count)
    MultipleFinalMarkings(usize),
    /// A token count or arc weight is not a number in `0..=u32::MAX` (with the text)
    InvalidTokenCount(String),
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
}

impl std::fmt::Display for PNMLParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to parse PNML: {self:?}")
    }
}

impl std::error::Error for PNMLParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PNMLParseError::IOError(e) => Some(e.as_ref()),
            PNMLParseError::XMLParsingError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for PNMLParseError {
    fn from(e: std::io::Error) -> Self {
        PNMLParseError::IOError(std::sync::Arc::new(e))
    }
}

impl From<QuickXMLError> for PNMLParseError {
    fn from(e: QuickXMLError) -> Self {
        Self::XMLParsingError(e)
    }
}

const IN_TRANSITION: &str = "a transition is read in this mode";

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
    FinalMarkings,
    FinalMarking,
    FinalMarkingPlace,
}

/// A transition as read, added to the net once its label is known
struct TransitionInfo {
    pnml_id: String,
    name_text: Option<String>,
    silent: bool,
}

#[derive(Clone, Copy)]
enum Node {
    Place(PlaceID),
    Transition(TransitionID),
}

fn attribute(element: &BytesStart<'_>, key: &'static str) -> Result<String, PNMLParseError> {
    let value = element
        .try_get_attribute(key)
        .map_err(QuickXMLError::from)?
        .ok_or(PNMLParseError::MissingKey(key))?
        .value;
    Ok(read_xml_text_unescaped(&mut value.as_ref()))
}

fn token_count(text: String) -> Result<u32, PNMLParseError> {
    text.parse()
        .map_err(|_| PNMLParseError::InvalidTokenCount(text))
}

///
/// Import a PNML file from the given XML reader ([`quick_xml::Reader`])
///
/// Returns the net, its initial marking (`None` if no place is marked) and its final marking
/// (`None` without `ProM`'s `<finalmarkings>` extension). Use [`AcceptingPetriNet::import_pnml`]
/// to require both markings, or [`PetriNet::import_pnml`] for the structure alone.
///
/// Places and transitions get ids in file order, and each node's PNML id becomes its name. A
/// transition is silent if `ProM`'s `toolspecific` tag marks it invisible; otherwise its label is
/// the text of its `<name>`, or its PNML id if it has no name. Arcs between the same nodes merge.
///
/// Inhibitor, reset and other special arcs are not supported, and neither are several final
/// markings.
///
/// [`AcceptingPetriNet::import_pnml`]: crate::core::process_models::case_centric::petri_net::AcceptingPetriNet::import_pnml
/// [`PetriNet::import_pnml`]: crate::core::process_models::case_centric::petri_net::PetriNet::import_pnml
pub fn import_pnml<T: BufRead>(
    reader: &mut Reader<T>,
) -> Result<(PetriNet, Option<Marking>, Option<Marking>), PNMLParseError> {
    reader.config_mut().trim_text(true);
    reader.config_mut().expand_empty_elements = true;
    let mut buf = Vec::new();

    let mut mode = Mode::None;
    let mut encountered_pnml_tag = false;
    let mut net = PetriNet::new();
    let mut transitions: Vec<TransitionInfo> = Vec::new();
    let mut place_ids: HashMap<String, PlaceID> = HashMap::new();
    let mut arcs: Vec<(String, String, u32)> = Vec::new();
    let mut initial_marking = Marking::new();
    let mut final_markings: Vec<Vec<(String, u32)>> = Vec::new();
    let mut current_place: Option<PlaceID> = None;
    let mut current_ref: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf)? {
            quick_xml::events::Event::Start(b) => match (b.name().as_ref(), mode) {
                (b"pnml", _) => encountered_pnml_tag = true,
                (b"net" | b"page", _) => mode = Mode::Net,
                (b"place", Mode::FinalMarking) => {
                    current_ref = Some(attribute(&b, "idref")?);
                    mode = Mode::FinalMarkingPlace;
                }
                (b"place", _) => {
                    let pnml_id = attribute(&b, "id")?;
                    let id = net.add_place(Some(pnml_id.clone()));
                    if place_ids.insert(pnml_id.clone(), id).is_some() {
                        return Err(PNMLParseError::DuplicateNode(pnml_id));
                    }
                    current_place = Some(id);
                    mode = Mode::Place;
                }
                (b"transition", _) => {
                    transitions.push(TransitionInfo {
                        pnml_id: attribute(&b, "id")?,
                        name_text: None,
                        silent: false,
                    });
                    mode = Mode::Transition;
                }
                (b"arc", _) => {
                    arcs.push((attribute(&b, "source")?, attribute(&b, "target")?, 1));
                    mode = Mode::Arc;
                }
                (b"inscription", Mode::Arc) => mode = Mode::ArcInscription,
                (b"toolspecific", Mode::Transition) => {
                    let attr = b
                        .try_get_attribute("activity")
                        .map_err(QuickXMLError::from)?;
                    if attr.is_some_and(|attr| attr.value.as_ref() == b"$invisible$") {
                        transitions.last_mut().expect(IN_TRANSITION).silent = true;
                    }
                }
                (b"initialMarking", Mode::Place) => mode = Mode::InitialMarking,
                (b"finalmarkings", _) => mode = Mode::FinalMarkings,
                (b"marking", Mode::FinalMarkings) => {
                    final_markings.push(Vec::new());
                    mode = Mode::FinalMarking;
                }
                (b"name", Mode::Place) => mode = Mode::PlaceName,
                (b"name", Mode::Transition) => mode = Mode::TransitionName,
                _ => {}
            },
            quick_xml::events::Event::End(b) => match (b.name().as_ref(), mode) {
                (b"place", Mode::FinalMarkingPlace) => mode = Mode::FinalMarking,
                (b"place" | b"transition" | b"arc", _) => mode = Mode::Net,
                (b"initialMarking", Mode::InitialMarking) => mode = Mode::Place,
                (b"inscription", Mode::ArcInscription) => mode = Mode::Arc,
                (b"marking", Mode::FinalMarking) => mode = Mode::FinalMarkings,
                (b"finalmarkings", _) => mode = Mode::Net,
                (b"name", Mode::PlaceName) => mode = Mode::Place,
                (b"name", Mode::TransitionName) => mode = Mode::Transition,
                _ => {}
            },
            quick_xml::events::Event::Text(t) => {
                let text = read_xml_text_unescaped(&mut t.as_ref());
                match mode {
                    Mode::TransitionName => {
                        transitions.last_mut().expect(IN_TRANSITION).name_text = Some(text);
                    }
                    Mode::InitialMarking => {
                        let place = current_place.expect("a place is read in this mode");
                        initial_marking.add(place, token_count(text)?);
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
                    _ => {}
                }
            }
            quick_xml::events::Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if !encountered_pnml_tag {
        return Err(PNMLParseError::NoPNMLTag);
    }

    let mut nodes: HashMap<String, Node> = place_ids
        .iter()
        .map(|(pnml_id, place)| (pnml_id.clone(), Node::Place(*place)))
        .collect();
    for t in transitions {
        let label = (!t.silent).then(|| t.name_text.unwrap_or_else(|| t.pnml_id.clone()));
        let id = net.add_transition(label, Some(t.pnml_id.clone()));
        if nodes
            .insert(t.pnml_id.clone(), Node::Transition(id))
            .is_some()
        {
            return Err(PNMLParseError::DuplicateNode(t.pnml_id));
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
            _ => return Err(PNMLParseError::InvalidArc { source, target }),
        };
        net.add_arc(from_to, Some(weight));
    }

    let initial_marking = (!initial_marking.is_empty()).then_some(initial_marking);
    if final_markings.len() > 1 {
        return Err(PNMLParseError::MultipleFinalMarkings(final_markings.len()));
    }
    let final_marking = final_markings
        .pop()
        .map(|marking| {
            marking
                .into_iter()
                .map(|(pnml_id, tokens)| match place_ids.get(&pnml_id) {
                    Some(place) => Ok((*place, tokens)),
                    None => Err(PNMLParseError::UnknownNode(pnml_id)),
                })
                .collect::<Result<Marking, _>>()
        })
        .transpose()?;
    Ok((net, initial_marking, final_marking))
}

/// The accepting net of what [`import_pnml`] read, if both markings are present
pub(crate) fn accepting_net(
    (net, initial_marking, final_marking): (PetriNet, Option<Marking>, Option<Marking>),
) -> Result<AcceptingPetriNet, PNMLParseError> {
    Ok(AcceptingPetriNet::new(
        net,
        initial_marking.ok_or(PNMLParseError::MissingInitialMarking)?,
        final_marking.ok_or(PNMLParseError::MissingFinalMarking)?,
    ))
}

///
/// Import a PNML file from the given standard buffered reader (implementing [`std::io::BufRead`])
///
/// See [`import_pnml`] for what is read.
pub fn import_pnml_reader<T: BufRead>(
    std_reader: &mut T,
) -> Result<(PetriNet, Option<Marking>, Option<Marking>), PNMLParseError> {
    import_pnml(&mut Reader::from_reader(std_reader))
}

/// Import a PNML file from the given filepath
///
/// See [`import_pnml`] for what is read.
pub fn import_pnml_from_path<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<(PetriNet, Option<Marking>, Option<Marking>), PNMLParseError> {
    import_pnml(&mut Reader::from_file(path)?)
}

#[cfg(test)]
mod tests {
    use super::{import_pnml_from_path, import_pnml_reader, PNMLParseError};
    use crate::{
        core::process_models::case_centric::petri_net::{
            marking::Marking,
            net::{PlaceID, TransitionID},
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

    #[test]
    fn reads_labels_silent_flags_and_file_order() {
        let (net, initial, final_marking) = import_pnml_reader(&mut SMALL.as_bytes()).unwrap();
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
            import_pnml_reader(&mut pnml.as_bytes()),
            Err(PNMLParseError::MultipleFinalMarkings(2))
        ));
    }

    #[test]
    fn references_to_unknown_nodes_are_an_error() {
        let arc = SMALL.replace(r#"target="sink"/>"#, r#"target="nowhere"/>"#);
        assert!(matches!(
            import_pnml_reader(&mut arc.as_bytes()),
            Err(PNMLParseError::UnknownNode(id)) if id == "nowhere"
        ));
        let marking = SMALL.replace(r#"<place idref="sink">"#, r#"<place idref="nowhere">"#);
        assert!(matches!(
            import_pnml_reader(&mut marking.as_bytes()),
            Err(PNMLParseError::UnknownNode(id)) if id == "nowhere"
        ));
    }

    #[test]
    fn duplicate_ids_are_an_error() {
        let places = SMALL.replace(r#"<place id="sink"/>"#, r#"<place id="src"/>"#);
        assert!(matches!(
            import_pnml_reader(&mut places.as_bytes()),
            Err(PNMLParseError::DuplicateNode(id)) if id == "src"
        ));
        let mixed = SMALL.replace(
            r#"<transition id="t_noname"/>"#,
            r#"<transition id="sink"/>"#,
        );
        assert!(matches!(
            import_pnml_reader(&mut mixed.as_bytes()),
            Err(PNMLParseError::DuplicateNode(id)) if id == "sink"
        ));
    }

    #[test]
    fn arcs_between_places_are_an_error() {
        let pnml = SMALL.replace(
            r#"source="t_b" target="sink""#,
            r#"source="src" target="sink""#,
        );
        assert!(matches!(
            import_pnml_reader(&mut pnml.as_bytes()),
            Err(PNMLParseError::InvalidArc { .. })
        ));
    }

    #[test]
    fn invalid_token_count_is_an_error() {
        let pnml = SMALL.replace(
            "<initialMarking><text>1</text>",
            "<initialMarking><text>abc</text>",
        );
        assert!(matches!(
            import_pnml_reader(&mut pnml.as_bytes()),
            Err(PNMLParseError::InvalidTokenCount(_))
        ));
    }

    #[test]
    fn pnml_import() {
        let (net, initial, final_marking) =
            import_pnml_from_path(petri_net_file("pn.pnml")).unwrap();
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
            import_pnml_from_path(petri_net_file("bpic12-tsinghua.pnml")).unwrap();
        assert_eq!(net.num_transitions(), 10);
        assert_eq!((initial, final_marking), (None, None));
    }

    #[test]
    fn import_is_deterministic() {
        let path = petri_net_file("sepsis-DISCovered.apnml");
        assert_eq!(
            import_pnml_from_path(&path).unwrap(),
            import_pnml_from_path(&path).unwrap()
        );
    }

    #[test]
    fn not_a_pnml_file_is_an_error() {
        assert!(import_pnml_from_path(petri_net_file("not-a-petri-net.slang")).is_err());
    }
}
