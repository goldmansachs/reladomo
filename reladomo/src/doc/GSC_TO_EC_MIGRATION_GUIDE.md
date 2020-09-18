# User Guide to migrate from GS Collections to Eclipse Collections

Starting with version 17.0.0, `Reladomo` supports [Eclipse Collections](https://github.com/eclipse/eclipse-collections).
GS Collections support is deprecated in 17.x.x. The GS Collections support is removed starting with 18.0.0.
At this moment all users need to migrate migrate from GS Collections to Eclipse Collections.

Here is a quick guide to migrate from GS Collections to Eclipse Collections

### General Migration Guide
1. In `reladomo-gen` task in your build configuration (Ant, Maven or Gradle), replace `generateGscListMethod` with `generateEcListMethod`

**Ant example:**
```
    <reladomo-gen xml="PATH_TO_THE_XML"
        generatedDir="PATH_TO_THE_GEN_DIR"
        nonGeneratedDir="PATH_TO_THE_NON_GEN_DIR"
        generateGscListMethod="true" />
```
```
    <reladomo-gen xml="PATH_TO_THE_XML"
        generatedDir="PATH_TO_THE_GEN_DIR"
        nonGeneratedDir="PATH_TO_THE_NON_GEN_DIR"
        generateEcListMethod="true" />
```

2. Replace `asGscList()` with `asEcList()` in your code. 
3. Replace `com.gs.collections.*` imports with `org.eclipse.collections.*` in your code

### Breaking Changes in 17.0.0
As part of version `17.0.0`,  we made breaking changes in several minor public APIs in Reladomo. If you use any of
these APIs in conjunction with GS Collections APIs, you may need to replace GS Collections dependency with Eclipse Collections equivalent.

##### Classes
- `MithraFastList`
- `AdhocFastList`
- `ByteArraySet`
- `ConstantIntSet`
- `ConstantShortSet`

##### Methods
- `AbstractDatedCache.MatchAllAsOfDatesProcedureForMany#getResult()`
- `MasterSyncResult#getBuffers()`
- `MithraCompositeList#getLists()`
- `AsOfEqualityChecker#getMsiPool()`
- `ConcurrentIntObjectHashMap#parallelForEachValue()`
- All methods in `ListFactory`
