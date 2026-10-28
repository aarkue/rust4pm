//! Drawings of real logs written to `test_data/export`, to check by eye.

use std::collections::BTreeMap;

use graphlay::Direction;

use crate::core::event_data::case_centric::utils::activity_projection::EventLogActivityProjection;
use crate::core::event_data::case_centric::xes::import_xes::{import_xes_path, XESImportOptions};
use crate::core::event_data::object_centric::linked_ocel::{IndexLinkedOCEL, SlimLinkedOCEL};
use crate::core::event_data::object_centric::type_graph::OcTypeGraph;
use crate::core::process_models::object_centric::ocdfg::object_centric_dfg_struct::OCDirectlyFollowsGraph;
use crate::discovery::case_centric::alphappp::full::{discover_alphappp_net, AlphaPPPConfig};
use crate::discovery::case_centric::dfg::discover_dfg;
use crate::discovery::case_centric::inductive_miner::{
    discover_inductive_net, InductiveMinerDfgOptions, InductiveMinerPreset, InductiveMinerVariant,
};
use crate::discovery::object_centric::ocpn::{discover_ocpn_from, OcpnDiscoveryOptions};
use crate::test_utils::{get_test_data_path, write_export};
use crate::visualization::annotation::EdgeAnnotation;
use crate::visualization::dfg::{DfgBuildOptions, DfgNodeRef, DfgStyle};
use crate::visualization::engine::render_scene_svg;
use crate::visualization::oc_type_graph::OcTypeGraphBuildOptions;
use crate::visualization::ocdfg::{OcdfgBuildOptions, OcdfgStyle};
use crate::visualization::ocpn::{OcpnBuildOptions, OcpnStyle};
use crate::visualization::petri_net::{PetriBuildOptions, PetriNodeRef, PetriStyle};
use crate::visualization::styling::{ElementStyle, StyleOverrides};
use crate::visualization::{model, LayoutSettings};
use crate::{EventLog, Importable, OCEL};

fn xes(name: &str) -> EventLog {
    import_xes_path(
        get_test_data_path().join("xes").join(name),
        XESImportOptions::default(),
    )
    .unwrap()
}

fn ocel(name: &str) -> OCEL {
    OCEL::import_from_path(get_test_data_path().join("ocel").join(name)).unwrap()
}

#[test]
fn petri_nets() {
    for (log, file) in [
        ("RepairExample", "RepairExample.xes"),
        ("Sepsis", "Sepsis Cases - Event Log.xes.gz"),
    ] {
        let projection = EventLogActivityProjection::from(&xes(file));
        for (variant, name) in [
            (InductiveMinerVariant::Imf, "IMf"),
            (InductiveMinerVariant::Imd, "IMd"),
        ] {
            let net =
                discover_inductive_net(&projection, variant, 0.2, InductiveMinerPreset::Default);
            let svg =
                model::svg(&net, &PetriBuildOptions::default(), &PetriStyle::default()).unwrap();
            write_export(&format!("{log}-{name}-petri-net.svg"), &svg);
        }
    }

    let projection = EventLogActivityProjection::from(&xes("RepairExample.xes"));
    let net = discover_alphappp_net(&projection, AlphaPPPConfig::default());
    let svg = model::svg(&net, &PetriBuildOptions::default(), &PetriStyle::default()).unwrap();
    write_export("RepairExample-Alpha-petri-net.svg", &svg);
}

#[test]
fn petri_net_variants() {
    let projection = EventLogActivityProjection::from(&xes("RepairExample.xes"));
    let net = discover_inductive_net(
        &projection,
        InductiveMinerVariant::Imf,
        0.2,
        InductiveMinerPreset::Default,
    );

    let down = PetriBuildOptions {
        layout: LayoutSettings {
            direction: Some(Direction::TopBottom),
            ..LayoutSettings::default()
        },
        ..PetriBuildOptions::default()
    };
    let svg = model::svg(&net, &down, &PetriStyle::default()).unwrap();
    write_export("RepairExample-petri-net-top-bottom.svg", &svg);

    // One transition highlighted, everything else dimmed.
    let options = PetriBuildOptions::default();
    let layout = model::layout(&net, &options).unwrap();
    let refs = model::layout_request(&net, &options).unwrap().nodes;
    let selected = refs
        .iter()
        .position(|r| match r {
            PetriNodeRef::Transition(t) => {
                net.net.transition(*t).label.as_deref() == Some("Analyze Defect")
            }
            PetriNodeRef::Place(_) => false,
        })
        .unwrap();
    let mut overrides = StyleOverrides::default();
    for i in 0..layout.nodes.len() {
        let style = if i == selected {
            ElementStyle {
                stroke: Some("#d62728".into()),
                stroke_width: Some(3.0),
                extra_label: Some("selected".into()),
                ..ElementStyle::default()
            }
        } else {
            ElementStyle {
                opacity: Some(0.35),
                ..ElementStyle::default()
            }
        };
        overrides.nodes.insert(i, style);
    }
    let scene =
        model::scene_from_layout(&net, &layout, &options, &PetriStyle::default(), &overrides)
            .unwrap();
    write_export(
        "RepairExample-petri-net-highlighted.svg",
        &render_scene_svg(&scene, None),
    );
}

#[test]
fn dfg_variants() {
    let log = xes("RepairExample.xes");
    let dfg = discover_dfg(&log);

    let across = DfgBuildOptions {
        layout: LayoutSettings {
            direction: Some(Direction::LeftRight),
            ..LayoutSettings::default()
        },
        ..DfgBuildOptions::default()
    };
    write_export(
        "RepairExample-DFG-left-right.svg",
        &model::svg(&dfg, &across, &DfgStyle::default()).unwrap(),
    );

    // Each edge labelled with its share of all transitions, the three most frequent drawn bold.
    let total: u64 = dfg
        .directly_follows_relations
        .values()
        .map(|&c| c as u64)
        .sum();
    let mut ranked: Vec<_> = dfg.directly_follows_relations.iter().collect();
    ranked.sort_by_key(|(_, &c)| std::cmp::Reverse(c));
    let mut edges = BTreeMap::new();
    for (rank, ((from, to), &count)) in ranked.into_iter().enumerate() {
        edges.insert(
            (
                DfgNodeRef::Activity(from.to_string()),
                DfgNodeRef::Activity(to.to_string()),
            ),
            EdgeAnnotation {
                label: Some(format!("{:.1}%", 100.0 * count as f64 / total as f64)),
                thickness: (rank < 3).then_some(6.0),
                ..EdgeAnnotation::default()
            },
        );
    }
    let mut annotated = DfgBuildOptions::default();
    annotated.annotations.edges = edges;
    write_export(
        "RepairExample-DFG-annotated.svg",
        &model::svg(&dfg, &annotated, &DfgStyle::default()).unwrap(),
    );
}

#[test]
fn object_centric_models() {
    for (name, file) in [
        ("ContainerLogistics", "ContainerLogistics.json"),
        ("p2p", "ocel2-p2p.json"),
    ] {
        let ocel = ocel(file);

        let indexed = IndexLinkedOCEL::from(ocel.clone());
        let ocdfg = OCDirectlyFollowsGraph::create_from_ocel(&indexed);
        let svg = model::svg(
            &ocdfg,
            &OcdfgBuildOptions::default(),
            &OcdfgStyle::default(),
        )
        .unwrap();
        write_export(&format!("{name}-OCDFG.svg"), &svg);

        let locel = SlimLinkedOCEL::from_ocel(ocel);
        let graph = OcTypeGraph::from_linked_ocel(&locel);
        let svg = model::svg(
            &graph,
            &OcTypeGraphBuildOptions::default(),
            &Default::default(),
        )
        .unwrap();
        write_export(&format!("{name}-type-graph.svg"), &svg);

        let ocpn = discover_ocpn_from(
            &locel,
            OcpnDiscoveryOptions::new(InductiveMinerDfgOptions::default()),
        );
        let svg = model::svg(
            &ocpn.to_json_form(),
            &OcpnBuildOptions::default(),
            &OcpnStyle::default(),
        )
        .unwrap();
        write_export(&format!("{name}-ocpn-IMd.svg"), &svg);
    }

    let locel = SlimLinkedOCEL::from_ocel(ocel("order-management.json"));
    let options = OcpnDiscoveryOptions::default().with_activities([
        "place order",
        "confirm order",
        "pay order",
        "create package",
        "send package",
        "package delivered",
    ]);
    let ocpn = discover_ocpn_from(&locel, options);
    let svg = model::svg(
        &ocpn.to_json_form(),
        &OcpnBuildOptions::default(),
        &OcpnStyle::default(),
    )
    .unwrap();
    write_export("order-management-ocpn-some-activities.svg", &svg);
}
