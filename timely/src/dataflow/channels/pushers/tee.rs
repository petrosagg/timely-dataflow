//! A `Push` implementor with a list of `Box<Push>` to forward pushes to.

use std::cell::RefCell;
use std::fmt::{self, Debug};
use std::rc::Rc;

use crate::dataflow::channels::{BundleCore, Message};

use crate::communication::Push;
use crate::{Container, Data};

type PushImpl<T, D> = fn(&mut [Box<dyn Push<BundleCore<T, D>>>], &mut D, &mut Option<BundleCore<T, D>>);
type PushList<T, D> = Vec<Box<dyn Push<BundleCore<T, D>>>>;

/// Wraps a shared list of `Box<Push>` to forward pushes to. Owned by `Stream`.
pub struct TeeCore<T, D> {
    buffer: D,
    shared: Rc<RefCell<(PushImpl<T, D>, PushList<T, D>)>>,
}

/// [TeeCore] specialized to `Vec`-based container.
pub type Tee<T, D> = TeeCore<T, Vec<D>>;

fn push_single<T, D>(pushers: &mut [Box<dyn Push<BundleCore<T, D>>>], _buffer: &mut D, message: &mut Option<BundleCore<T, D>>) {
    assert!(pushers.len() <= 1);
    if let Some(pusher) = pushers.get_mut(0) {
        pusher.push(message);
    }
}

fn push_list<T: Data, D: Container>(pushers: &mut [Box<dyn Push<BundleCore<T, D>>>], buffer: &mut D, message: &mut Option<BundleCore<T, D>>) {
    if let Some(message) = message {
        for index in 1..pushers.len() {
            buffer.clone_from(&message.data);
            Message::push_at(buffer, message.time.clone(), &mut pushers[index-1]);
        }
    }
    else {
        for index in 1..pushers.len() {
            pushers[index-1].push(&mut None);
        }
    }
    if pushers.len() > 0 {
        let last = pushers.len() - 1;
        pushers[last].push(message);
    }
}

impl<T, D> Push<BundleCore<T, D>> for TeeCore<T, D> {
    #[inline]
    fn push(&mut self, message: &mut Option<BundleCore<T, D>>) {
        let (push_impl, ref mut pushers) = &mut *self.shared.borrow_mut();
        (push_impl)(pushers, &mut self.buffer, message)
    }
}

impl<T, D: Container> TeeCore<T, D> {
    /// Allocates a new pair of `Tee` and `TeeHelper`.
    pub fn new() -> (TeeCore<T, D>, TeeHelper<T, D>) {
        let shared = Rc::new(RefCell::new((push_single as PushImpl<T, D>, Vec::new())));
        let port = TeeCore {
            buffer: Default::default(),
            shared: shared.clone(),
        };

        (port, TeeHelper { shared })
    }
}

impl<T, D: Container> Clone for TeeCore<T, D> {
    fn clone(&self) -> Self {
        Self {
            buffer: Default::default(),
            shared: self.shared.clone(),
        }
    }
}

impl<T, D> Debug for TeeCore<T, D>
where
    D: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Tee");
        debug.field("buffer", &self.buffer);

        if let Ok(shared) = self.shared.try_borrow() {
            debug.field("shared", &format!("{} pushers", shared.1.len()));
        } else {
            debug.field("shared", &"...");
        }

        debug.finish()
    }
}

/// A shared list of `Box<Push>` used to add `Push` implementors.
pub struct TeeHelper<T, D> {
    shared: Rc<RefCell<(PushImpl<T, D>, PushList<T, D>)>>,
}

impl<T, D> TeeHelper<T, D> {
    /// Adds a new `Push` implementor to the list of recipients shared with a `Stream`.
    pub fn add_pusher<P: Push<BundleCore<T, D>>+'static>(self, pusher: P) {
        self.shared.borrow_mut().1.push(Box::new(pusher));
    }
}

impl<T: Data, D: Container> Clone for TeeHelper<T, D> {
    fn clone(&self) -> Self {
        self.shared.borrow_mut().0 = push_list;
        TeeHelper {
            shared: self.shared.clone(),
        }
    }
}

impl<T, D> Debug for TeeHelper<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("TeeHelper");

        if let Ok(shared) = self.shared.try_borrow() {
            debug.field("shared", &format!("{} pushers", shared.1.len()));
        } else {
            debug.field("shared", &"...");
        }

        debug.finish()
    }
}
