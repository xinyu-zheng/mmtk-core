initSidebarItems({"struct":[["Mutator","A mutator is a per-thread data structure that manages allocations and barriers. It is usually highly coupled with the language VM. It is recommended for MMTk users 1) to have a mutator struct of the same layout in the thread local storage that can be accessed efficiently, and 2) to implement fastpath allocation and barriers for the mutator in the VM side."],["MutatorConfig",""]],"trait":[["MutatorContext","Each GC plan should provide their implementation of a MutatorContext. Note that this trait is no longer needed as we removed per-plan mutator implementation and we will remove this trait as well in the future."]],"type":[["SpaceMapping",""]]});