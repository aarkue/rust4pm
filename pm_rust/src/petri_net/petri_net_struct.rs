struct Place {

}

struct Transition {

}
enum ArcType {
  PlaceTransition(Place,Transition),
  TransitionPlace(Transition, Place)
}

struct Arc {

}

struct PetriNet {
  places: Vec<Place>,
  transitions: Vec<ArcType>,
  
}