use std::sync::atomic::AtomicU32;

pub static ID_GENERATOR: IdGen = IdGen::new();

#[derive(Default)]
pub struct IdGen(AtomicU32);

impl IdGen {
    const fn new() -> Self {
        Self(AtomicU32::new(0))
    }

    pub fn next_id(&self) -> u32 {
        use std::sync::atomic::Ordering;
        self.0.fetch_add(1, Ordering::AcqRel)
    }
}
