/// A wrapper for either an owned or mutable reference to a [`quick_xml::Writer`]
#[allow(missing_debug_implementations)]
pub enum XMLWriterWrapper<'a, W> {
    /// Owned [`quick_xml::Writer`]
    Owned(quick_xml::Writer<W>),
    /// Mutable Reference to [`quick_xml::Writer`]
    Ref(&'a mut quick_xml::Writer<W>),
}

impl<'a, W> XMLWriterWrapper<'a, W> {
    /// Return a mutable reference to a [`quick_xml::Writer`]
    pub fn to_xml_writer(&'a mut self) -> &'a mut quick_xml::Writer<W> {
        match self {
            XMLWriterWrapper::Owned(w) => w,
            XMLWriterWrapper::Ref(w) => w,
        }
    }
}

impl<W: std::io::Write> From<W> for XMLWriterWrapper<'_, W> {
    fn from(w: W) -> Self {
        Self::Owned(quick_xml::Writer::new(w))
    }
}

impl<'a, W> From<&'a mut quick_xml::Writer<W>> for XMLWriterWrapper<'a, W> {
    fn from(w: &'a mut quick_xml::Writer<W>) -> Self {
        Self::Ref(w)
    }
}

// Not used yet, but maybe useful in the future:

// /// A wrapper for either an owned or mutable reference to a [`quick_xml::Reader`]
// #[allow(missing_debug_implementations)]
// pub enum XMLReaderWrapper<'a, R> {
//     /// Owned [`quick_xml::Reader`]
//     Owned(quick_xml::Reader<R>),
//     /// Mutable Reference to [`quick_xml::Reader`]
//     Ref(&'a mut quick_xml::Reader<R>),
// }

// impl<'a, R> XMLReaderWrapper<'a, R> {
//     /// Return a mutable reference to a [`quick_xml::Reader`]
//     pub fn to_xml_reader(&'a mut self) -> &mut quick_xml::Reader<R> {
//         match self {
//             XMLReaderWrapper::Owned(r) => r,
//             XMLReaderWrapper::Ref(r) => r,
//         }
//     }
// }

// impl<'a, R: std::io::Read> From<R> for XMLReaderWrapper<'a, R> {
//     fn from(r: R) -> Self {
//         Self::Owned(quick_xml::Reader::from_reader(r))
//     }
// }

// impl<'a, R> From<&'a mut quick_xml::Reader<R>> for XMLReaderWrapper<'a, R> {
//     fn from(w: &'a mut quick_xml::Reader<R>) -> Self {
//         Self::Ref(w)
//     }
// }
