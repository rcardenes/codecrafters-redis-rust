use sha1::{Sha1, Digest};

#[derive(Clone)]
pub struct ReplicaInfo {
    hasher: Sha1,
    offset: usize,
}

impl ReplicaInfo {
    pub fn new() -> Self {
        ReplicaInfo {
            hasher: Sha1::new(),
            offset: 0,
        }
    }

    pub fn digest_string(&self) -> String {
        let cl = self.hasher.clone();
        let digest = cl.finalize();

        format!("{digest:x}")
    }
    
    pub fn offset(&self) -> usize {
        self.offset
    }
}
