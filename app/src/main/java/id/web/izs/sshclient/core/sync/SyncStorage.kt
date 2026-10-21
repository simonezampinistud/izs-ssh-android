package id.web.izs.sshclient.core.sync

/** Storage boundary for the sync engine; implementations protect YAML writes with this lock. */
interface SyncStorage {
    fun loadYaml(): String?
    fun saveYaml(yaml: String)
    fun dropLegacySyncTarget()
    fun loadKnownHostsJson(): String?
    fun saveKnownHostsJson(json: String)
    fun savePreImport(yaml: String, lastChange: String)
    fun loadPreImportYaml(): String?
    fun loadPreImportStamp(): String
    var auto: Boolean
    var partsHotkeys: Boolean
    var partsAppearance: Boolean
    var partsVault: Boolean
    var lastRemoteChange: String
    var lastSyncedHash: String
    var syncConflict: Boolean

    /** Short, reentrant critical section; never hold it across network requests. */
    fun <T> withSyncStateLock(block: () -> T): T
}
