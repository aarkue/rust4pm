use super::petri_net_struct::{ArcType, Marking, PlaceID};
use crate::PetriNet;
use quick_xml::{Error as QuickXMLError, Reader};
use std::{collections::HashMap, io::BufRead};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    None,
    Pnml,
    Net,
    Place,
    Transition,
    PlaceName,
    TransitionName,
    InitialMarking,
    FinalMarkings,
    FinalMarkingsMarking,
    FinalMarkingMarkingPlace,
    Arc,
    ArcInscription,
}

fn read_to_string(x: &mut &[u8]) -> String {
    String::from_utf8_lossy(x).to_string()
}

///
/// Error encountered while parsing PNML
///
#[derive(Debug, Clone)]
pub enum PNMLParseError {
    /// Encountered PNML/XML tag unexpected for the current parsing mode
    InvalidMode,
    /// IO error
    IOError(std::rc::Rc<std::io::Error>),
    /// XML error (e.g., incorrect XML format )
    XMLParsingError(QuickXMLError),
    /// Missing key on XML element (with expected key included)
    MissingKey(&'static str),
    /// Invalid value of XML attribute with key (with key included)
    InvalidKeyValue(&'static str),
    /// Encountered no PNML tag (i.e., the parsed data was not a PNML file)
    NoPNMLTag,
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

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

impl From<std::io::Error> for PNMLParseError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(std::rc::Rc::new(e))
    }
}

impl From<QuickXMLError> for PNMLParseError {
    fn from(e: QuickXMLError) -> Self {
        Self::XMLParsingError(e)
    }
}

///
/// Import a PNML file from the given XML reader ([`quick_xml::Reader`])
///
/// Also consider using [`PetriNet::import_pnml`] for importing from a filepath directly for convenience.
///
/// Note, that this implementation (at least currently) is a best-effort parser for the basic types of Petri nets encountered commonly in Process Mining.
/// In particular, the following Petri net features are implemented:
///
/// - Places, Transitions
/// - Silent transitions (toolspecific tag)
/// - Basics arcs (i.e., not inhibitor arcs, etc.)
/// - Arc weights
/// - A single initial marking
/// - Multiple final markings
///
///
/// Also see [`import_pnml_reader`] for an alternative version of this function, which takes a (standard) buffered reader implementing [`std::io::BufRead`] instead
pub fn import_pnml<T>(reader: &mut Reader<T>) -> Result<PetriNet, PNMLParseError>
where
    T: BufRead,
{
    reader.config_mut().trim_text(true);
    reader.config_mut().expand_empty_elements = true;
    let mut buf: Vec<u8> = Vec::new();

    let mut current_mode: Mode = Mode::None;
    let mut encountered_pnml_tag = false;
    let mut pn = PetriNet::new();
    let mut initial_marking: Marking = HashMap::new();
    let mut final_markings: Vec<Marking> = vec![];

    let mut id_map: HashMap<String, Uuid> = HashMap::new();

    let mut current_id: Option<Uuid> = None;

    let mut arcs: Vec<(String, String, u32)> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            quick_xml::events::Event::Start(b) => match b.name().as_ref() {
                b"pnml" => {
                    current_mode = Mode::Pnml;
                    encountered_pnml_tag = true;
                }
                b"net" => {
                    if current_mode != Mode::Pnml {
                        eprintln!("Expected to be in Mode::PNML when encountering net");
                    }
                    current_mode = Mode::Net;
                }
                b"page" => {
                    if current_mode != Mode::Net {
                        eprintln!("Expected to be in Mode::Net when encountering page");
                    }
                    current_mode = Mode::Net;
                }
                b"place" => {
                    if current_mode == Mode::FinalMarkingsMarking {
                        // Final Marking place
                        let id_ref = read_to_string(
                            &mut b
                                .try_get_attribute("idref")
                                .unwrap_or_default()
                                .ok_or(PNMLParseError::MissingKey("idref"))?
                                .value
                                .as_ref(),
                        );
                        current_mode = Mode::FinalMarkingMarkingPlace;
                        // Save idref as current_id (and use it when parsing the corresponding <text>1</text>)
                        current_id = id_map.get(&id_ref).cloned();
                    } else {
                        // Add place
                        current_mode = Mode::Place;
                        let place_id = b
                            .try_get_attribute("id")
                            .unwrap_or_default()
                            .ok_or(PNMLParseError::MissingKey("id"))?;
                        let place_id_str = read_to_string(&mut place_id.value.as_ref());
                        let uuid = Uuid::new_v4();
                        current_id = Some(uuid);
                        id_map.insert(place_id_str, uuid);
                        pn.add_place(Some(uuid));
                    }
                }
                b"transition" => {
                    current_mode = Mode::Transition;
                    let trans_id = b
                        .try_get_attribute("id")
                        .unwrap_or_default()
                        .ok_or(PNMLParseError::MissingKey("id"))?;
                    let trans_id_str = read_to_string(&mut trans_id.value.as_ref());
                    let uuid = Uuid::new_v4();
                    current_id = Some(uuid);
                    id_map.insert(trans_id_str, uuid);
                    pn.add_transition(Some(String::new()), Some(uuid));
                }
                b"arc" => {
                    let source_id = read_to_string(
                        &mut b
                            .try_get_attribute("source")
                            .unwrap_or_default()
                            .ok_or(PNMLParseError::MissingKey("source"))?
                            .value
                            .as_ref(),
                    );
                    let target_id = read_to_string(
                        &mut b
                            .try_get_attribute("target")
                            .unwrap_or_default()
                            .ok_or(PNMLParseError::MissingKey("target"))?
                            .value
                            .as_ref(),
                    );
                    // Only add arcs to vec here, and add them to the PetriNet only at the end
                    // bc. we do not know if the source/targets are encountered yet and we need to know which one is the transitions and which one the place
                    arcs.push((source_id, target_id, 1));
                    current_mode = Mode::Arc;
                }
                // Handle weighted arcs
                b"inscription" => {
                    if current_mode == Mode::Arc {
                        current_mode = Mode::ArcInscription;
                    }
                }
                // For handling silent transitions
                b"toolspecific" => {
                    if let Some(attr) = b.try_get_attribute("activity").unwrap_or_default().as_ref()
                    {
                        if attr.value.as_ref() == b"$invisible$" {
                            if let Some(trans) =
                                current_id.and_then(|id| pn.transitions.get_mut(&id))
                            {
                                // Set label to None (silent)
                                trans.label = None;
                            } else {
                                eprintln!(
                                    "Can't find current transition when adding toolspecific!"
                                );
                            }
                        }
                    }
                }
                // For handling initial markings
                b"initialMarking" => {
                    current_mode = Mode::InitialMarking;
                }
                b"finalmarkings" => current_mode = Mode::FinalMarkings,
                b"marking" => {
                    if current_mode == Mode::FinalMarkings {
                        current_mode = Mode::FinalMarkingsMarking;
                        // Add new final marking
                        final_markings.push(HashMap::new());
                    }
                }
                b"name" => match current_mode {
                    Mode::Place => current_mode = Mode::PlaceName,
                    Mode::Transition => current_mode = Mode::TransitionName,
                    _ => {}
                },
                _ => {}
            },
            quick_xml::events::Event::End(b) => match b.name().as_ref() {
                b"place" => {
                    if current_mode == Mode::FinalMarkingMarkingPlace {
                        current_mode = Mode::FinalMarkingsMarking;
                        current_id = None;
                    } else {
                        current_mode = Mode::Net;
                        current_id = None;
                    }
                }
                b"transition" => {
                    current_mode = Mode::Net;
                    current_id = None;
                }
                b"initialMarking" => {
                    current_mode = Mode::Place;
                }
                b"finalmarkings" => current_mode = Mode::Net,
                b"marking" => current_mode = Mode::FinalMarkings,
                b"inscription" => {
                    if current_mode == Mode::ArcInscription {
                        current_mode = Mode::Arc
                    }
                }
                b"arc" => {
                    current_mode = Mode::Net;
                }
                b"name" => match current_mode {
                    Mode::PlaceName => current_mode = Mode::Place,
                    Mode::TransitionName => current_mode = Mode::Transition,
                    _ => {}
                },
                _ => {}
            },
            quick_xml::events::Event::Text(t) => {
                let text = read_to_string(&mut t.as_ref());
                match current_mode {
                    Mode::TransitionName => {
                        if let Some(trans) = current_id.and_then(|id| pn.transitions.get_mut(&id)) {
                            // Only overwrite label if it is set to Some(...)
                            // Because this is what we do initially
                            // Otherwise, silent transitions might get labeled by accident
                            if trans.label.is_some() {
                                trans.label = Some(text);
                            }
                        } else {
                            eprintln!("Can't find current transition when adding text!");
                        }
                    }
                    Mode::InitialMarking => {
                        if let Some(place) = current_id.and_then(|id| pn.places.get(&id)) {
                            initial_marking
                                .insert(place.into(), text.parse::<u64>().unwrap_or_default());
                        }
                    }
                    Mode::FinalMarkingMarkingPlace => {
                        if let Some(place_id) = current_id {
                            if let Some(fm) = final_markings.last_mut() {
                                fm.insert(
                                    PlaceID(place_id),
                                    text.parse::<u64>().unwrap_or_default(),
                                );
                            }
                        }
                    }
                    Mode::ArcInscription => {
                        if let Some(arc) = arcs.last_mut() {
                            arc.2 = text.parse::<u32>().unwrap_or(1);
                        }
                    }
                    _ => {}
                }
            }
            quick_xml::events::Event::Eof => break,
            _ => {}
        }
    }

    if !encountered_pnml_tag {
        return Err(PNMLParseError::NoPNMLTag);
    }

    for (from, to, weight) in arcs {
        let from_uuid = id_map.get(&from);
        let to_uuid = id_map.get(&to);
        if let Some(from_uuid) = from_uuid {
            if let Some(to_uuid) = to_uuid {
                let mut from_to = None;
                // Option 1: Place -> Transition
                if let Some(place) = pn.places.get(from_uuid) {
                    if let Some(trans) = pn.transitions.get(to_uuid) {
                        from_to = Some(ArcType::place_to_transition(place.into(), trans.into()));
                    }
                // Option 2: Transition -> Place
                } else if let Some(trans) = pn.transitions.get(from_uuid) {
                    if let Some(place) = pn.places.get(to_uuid) {
                        from_to = Some(ArcType::transition_to_place(trans.into(), place.into()));
                    }
                }
                if let Some(from_to) = from_to {
                    pn.add_arc(from_to, Some(weight))
                }
            }
        }
    }
    if !initial_marking.is_empty() {
        pn.initial_marking = Some(initial_marking);
    }
    if !final_markings.is_empty() {
        pn.final_markings = Some(final_markings);
    }
    Ok(pn)
}

///
/// Import a PNML file from the given standard buffered reader (implementing [`std::io::BufRead`])
///
/// Also consider using [`PetriNet::import_pnml`] for importing from a filepath directly for convenience.
///
///
/// Also see [`import_pnml`] for an alternative version of this function, which takes a XML specific reader [`quick_xml::Reader`] instead
pub fn import_pnml_reader<T>(std_reader: &mut T) -> Result<PetriNet, PNMLParseError>
where
    T: BufRead,
{
    let mut xml_reader = Reader::from_reader(std_reader);
    import_pnml(&mut xml_reader)
}

///  Import a PNML file from the given filepath
///
/// Also consider using [`PetriNet::import_pnml`] for convenience or [`import_pnml`] for more control over the reader.
pub fn import_pnml_from_path<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<PetriNet, PNMLParseError> {
    import_pnml(&mut quick_xml::Reader::from_file(path)?)
}

#[cfg(test)]
mod test {
    use quick_xml::Reader;

    use crate::utils::test_utils::get_test_data_path;

    use super::import_pnml;

    #[test]
    fn test_pnml_import() {
        let path = get_test_data_path().join("petri-net").join("pn.pnml");
        let pn = import_pnml(&mut Reader::from_file(path).unwrap()).unwrap();
        assert_eq!(pn.transitions.len(), 46);
        assert_eq!(pn.places.len(), 9);
        assert_eq!(pn.arcs.len(), 24);
        assert!(pn.initial_marking.is_some());
        assert!(pn.final_markings.is_some());
        assert!(pn.arcs.iter().any(|arc| arc.weight == 1337));
        println!("{pn:#?}");
        #[cfg(feature = "graphviz-export")]
        {
            let svg_export_path = get_test_data_path().join("export").join("pn.svg");
            pn.export_svg(&svg_export_path).unwrap();
            println!("Exported to: file:///{}", svg_export_path.to_string_lossy());
        }
    }

    #[test]
    fn test_pnml_import_2() {
        let path = get_test_data_path()
            .join("petri-net")
            .join("bpic12-tsinghua.pnml");
        let pn_res = import_pnml(&mut Reader::from_file(path).unwrap());
        assert!(pn_res.is_ok());
        let pn = pn_res.unwrap();
        // assert_eq!(pn.transitions.len(), 46);
        // assert_eq!(pn.places.len(), 9);
        // assert_eq!(pn.arcs.len(), 24);
        // assert!(pn.initial_marking.is_some());
        // assert!(pn.final_markings.is_some());
        // assert!(pn.arcs.iter().any(|arc| arc.weight == 1337));
        // println!("{:#?}", pn);
        println!("Transitions: {:?}", pn.transitions);
        #[cfg(feature = "graphviz-export")]
        {
            let svg_export_path = get_test_data_path()
                .join("export")
                .join("bpic12-tsinghua.svg");
            pn.export_svg(&svg_export_path).unwrap();
            println!("Exported to: file:///{}", svg_export_path.to_string_lossy());
        }
    }

    #[test]
    fn test_invalid_pnml_import() {
        let path = get_test_data_path()
            .join("petri-net")
            .join("not-a-petri-net.slang");
        let pn_res = import_pnml(&mut Reader::from_file(path).unwrap());
        assert!(pn_res.is_err());
    }
}
