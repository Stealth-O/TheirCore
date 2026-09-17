import Foundation

/// The namespace that owns every public name in TheirCore and
/// TheirCoreTesting: `Their.Job`, `Their.Hub`, `Their.Lock`,
/// `Their.stress { ... }`.
///
/// Nothing here is mine. It's all theirs. The enum has no cases and is never
/// instantiated; it only groups declarations so they read as a phrase and never
/// collide with a consumer's own `Job`, `Hub` or `Lock`.
public enum Their {}
