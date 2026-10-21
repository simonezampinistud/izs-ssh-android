package id.web.izs.sshclient

import com.sun.net.httpserver.HttpServer
import id.web.izs.sshclient.core.config.RawConfigStore
import id.web.izs.sshclient.core.sync.SyncRepository
import id.web.izs.sshclient.core.sync.SyncConflictException
import id.web.izs.sshclient.core.sync.SyncStorage
import id.web.izs.sshclient.core.sync.TabbySyncApi
import id.web.izs.sshclient.core.sync.syncContentHash
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import okhttp3.OkHttpClient
import org.junit.Assert.*
import org.junit.Test
import java.net.InetSocketAddress
import java.util.Collections
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** Real sync repository + HTTP API against a loopback server and in-memory storage. */
class SyncRepositorySafetyTest {
    private val initial = """
        version: 8
        profiles:
          - id: ssh:demo
            type: ssh
            name: original
            options: {host: example.test, privateKeys: [vault://demo-key]}
        appearance: {theme: local-theme}
        futureExtension: {enabled: true}
        configSync: {host: 'https://sync.example.test', token: fake-test-token, configID: 7}
    """.trimIndent()
    private val edited get() = initial.replace("name: original", "name: edited-on-phone")

    private fun remote(content: String, stamp: String = "remote-new", id: Long = 7): String =
        buildJsonObject {
            put("id", id)
            put("name", "test config")
            put("content", content)
            put("modified_at", stamp)
        }.toString()

    private fun rejectsDownload(content: String) = runBlocking {
        val disk = MemoryStorage(initial)
        TestServer { Response(200, remote(content)) }.use { server ->
            val outcome = runCatching { repository(disk).downloadIntoLocal(server.host, "fake-token", 7) }
            assertTrue("Invalid remote content must fail", outcome.isFailure)
            assertEquals(initial, disk.loadYaml())
            assertEquals("baseline-stamp", disk.lastRemoteChange)
            assertEquals("baseline-hash", disk.lastSyncedHash)
            assertNull(disk.loadPreImportYaml())
        }
    }

    @Test fun `scalar remote download does not erase local profiles`() = rejectsDownload("not-a-config")
    @Test fun `list remote download does not erase local profiles`() = rejectsDownload("- wrong\n- root")
    @Test fun `invalid profiles shape does not erase local profiles`() = rejectsDownload("profiles: wrong")
    @Test fun `malformed YAML does not erase local profiles`() = rejectsDownload("profiles: [unclosed")
    @Test fun `incomplete encrypted shell does not replace local config`() = rejectsDownload("encrypted: true\nvault: {}")
    @Test fun `duplicate YAML keys do not replace local config`() = rejectsDownload("profiles: []\nprofiles: []")

    @Test fun `remote YAML diagnostics never include its contents`() {
        val error = runCatching { RawConfigStore.parseSync("profiles: [synthetic-secret-do-not-log") }.exceptionOrNull()!!
        assertFalse(error.toString().contains("synthetic-secret-do-not-log"))
        assertNull(error.cause)
    }

    @Test fun `failed remote reads never send PATCH`() = runBlocking {
        for (status in listOf(401, 404, 500)) {
            val disk = MemoryStorage(initial)
            TestServer { request ->
                if (request.method == "GET") Response(status, "unavailable") else Response(204)
            }.use { server ->
                assertTrue(runCatching {
                    repository(disk).uploadAsCurrent(server.host, "fake-token", 7, "test")
                }.isFailure)
                assertEquals("GET failure $status must abort", 0, server.requests.count { it.method == "PATCH" })
                assertEquals(initial, disk.loadYaml())
                assertEquals("baseline-hash", disk.lastSyncedHash)
                assertEquals("baseline-stamp", disk.lastRemoteChange)
            }
        }
    }

    @Test fun `invalid remote reads never send PATCH`() = runBlocking {
        for (content in listOf("profiles: [unclosed", "not-a-config", "profiles: wrong")) {
            val disk = MemoryStorage(initial)
            TestServer { request ->
                if (request.method == "GET") Response(200, remote(content)) else Response(204)
            }.use { server ->
                assertTrue(runCatching {
                    repository(disk).uploadAsCurrent(server.host, "fake-token", 7, "test")
                }.isFailure)
                assertEquals(0, server.requests.count { it.method == "PATCH" })
            }
        }
    }

    @Test fun `excluded sections absent remotely are not uploaded from local`() = runBlocking {
        val disk = MemoryStorage(initial).apply { partsAppearance = false }
        var uploaded = ""
        TestServer { request ->
            if (request.method == "PATCH") {
                uploaded = uploadedContent(request)
                Response(204)
            } else Response(200, remote(uploaded.ifBlank { "{}" }))
        }.use { server ->
            repository(disk).uploadAsCurrent(server.host, "fake-token", 7, "test")
            val document = RawConfigStore.loadRaw(uploaded)
            assertFalse(document.containsKey("appearance"))
            assertFalse(document.containsKey("configSync"))
            assertTrue(document.containsKey("futureExtension"))
            assertEquals(syncContentHash(disk.loadYaml()!!), disk.lastSyncedHash)
        }
    }

    @Test fun `server text field trimming does not create an upload conflict`() = runBlocking {
        val disk = MemoryStorage(initial)
        var uploaded = ""
        var stored = "{}"
        TestServer { request ->
            if (request.method == "PATCH") {
                uploaded = uploadedContent(request)
                stored = uploaded.trim()
                Response(204)
            } else Response(200, remote(stored))
        }.use { server ->
            repository(disk).uploadAsCurrent(server.host, "fake-token", 7, "test")
            assertNotEquals("The real server removes the dumper's trailing newline", uploaded, stored)
            assertEquals(RawConfigStore.parseSync(uploaded), RawConfigStore.parseSync(stored))
            assertEquals(initial, disk.loadYaml())
            assertEquals("remote-new", disk.lastRemoteChange)
            assertEquals(syncContentHash(initial), disk.lastSyncedHash)
            assertFalse(disk.syncConflict)
        }
    }

    @Test fun `equivalent server YAML formatting preserves successful upload`() = runBlocking {
        val disk = MemoryStorage(initial)
        var uploaded = ""
        var stored = "{}"
        TestServer { request ->
            if (request.method == "PATCH") {
                uploaded = uploadedContent(request)
                val reordered = linkedMapOf<String, Any?>()
                RawConfigStore.parseSync(uploaded).entries.reversed().forEach { (key, value) ->
                    reordered[key] = value
                }
                stored = RawConfigStore.dumpRaw(reordered).replace("\n", "\r\n")
                Response(204)
            } else Response(200, remote(stored))
        }.use { server ->
            repository(disk).uploadAsCurrent(server.host, "fake-token", 7, "test")
            assertNotEquals(uploaded, stored)
            assertEquals(initial, disk.loadYaml())
            assertEquals("remote-new", disk.lastRemoteChange)
            assertEquals(syncContentHash(initial), disk.lastSyncedHash)
            assertFalse(disk.syncConflict)
        }
    }

    @Test fun `whitespace changes inside a scalar still report an upload conflict`() = runBlocking {
        val original = initial + "\nfutureNote: 'keep trailing spaces '\n"
        val disk = MemoryStorage(original)
        var stored = "{}"
        TestServer { request ->
            if (request.method == "PATCH") {
                val changed = RawConfigStore.parseSync(uploadedContent(request))
                changed["futureNote"] = "keep trailing spaces"
                stored = RawConfigStore.dumpRaw(changed)
                Response(204)
            } else Response(200, remote(stored))
        }.use { server ->
            val result = runCatching {
                repository(disk).uploadAsCurrent(server.host, "fake-token", 7, "test")
            }
            assertTrue(result.exceptionOrNull() is SyncConflictException)
            assertEquals(original, disk.loadYaml())
            assertEquals("baseline-stamp", disk.lastRemoteChange)
            assertEquals("baseline-hash", disk.lastSyncedHash)
            assertTrue(disk.syncConflict)
        }
    }

    @Test fun `invalid server readback never marks an upload as synced`() = runBlocking {
        for (invalid in listOf("", "not-a-config", "profiles: wrong")) {
            val disk = MemoryStorage(initial)
            var patched = false
            TestServer { request ->
                if (request.method == "PATCH") {
                    patched = true
                    Response(204)
                } else Response(200, remote(if (patched) invalid else "{}"))
            }.use { server ->
                val result = runCatching {
                    repository(disk).uploadAsCurrent(server.host, "fake-token", 7, "test")
                }
                assertTrue(result.exceptionOrNull() is SyncConflictException)
                assertTrue(patched)
                assertEquals(initial, disk.loadYaml())
                assertEquals("baseline-stamp", disk.lastRemoteChange)
                assertEquals("baseline-hash", disk.lastSyncedHash)
                assertTrue(disk.syncConflict)
            }
        }
    }

    @Test fun `local edit during upload remains dirty and is never overwritten`() = runBlocking {
        for (configId in listOf(7L, 8L)) {
            val disk = MemoryStorage(initial)
            var uploaded = ""
            TestServer { request ->
                if (request.method == "PATCH") {
                    uploaded = uploadedContent(request)
                    disk.saveYaml(edited)
                    Response(204)
                } else Response(200, remote(uploaded.ifBlank { "{}" }, id = configId))
            }.use { server ->
                val outcome = runCatching {
                    repository(disk).uploadAsCurrent(server.host, "fake-token", configId, "test")
                }
                assertTrue("Changed local revision must be reported", outcome.isFailure)
                assertEquals("An upload must not erase an edit made during its HTTP request", edited, disk.loadYaml())
                assertEquals("baseline-hash", disk.lastSyncedHash)
                assertEquals("baseline-stamp", disk.lastRemoteChange)
                assertTrue(disk.syncConflict)
            }
        }
    }

    @Test fun `newer server content after PATCH is not marked as uploaded`() = runBlocking {
        val disk = MemoryStorage(initial)
        var patched = false
        TestServer { request ->
            if (request.method == "PATCH") {
                patched = true
                Response(204)
            } else Response(200, remote(if (patched) edited else "{}"))
        }.use { server ->
            assertTrue(runCatching {
                repository(disk).uploadAsCurrent(server.host, "fake-token", 7, "test")
            }.isFailure)
            assertEquals(initial, disk.loadYaml())
            assertEquals("baseline-stamp", disk.lastRemoteChange)
            assertEquals("baseline-hash", disk.lastSyncedHash)
            assertTrue(disk.syncConflict)
        }
    }

    @Test fun `local edit during download is not overwritten`() = runBlocking {
        val disk = MemoryStorage(initial)
        TestServer {
            disk.saveYaml(edited)
            Response(200, remote(initial))
        }.use { server ->
            assertTrue(runCatching {
                repository(disk).downloadIntoLocal(server.host, "fake-token", 7)
            }.isFailure)
            assertEquals(edited, disk.loadYaml())
            assertEquals("baseline-hash", disk.lastSyncedHash)
            assertNull(disk.loadPreImportYaml())
        }
    }

    @Test fun `auto download rechecks the local revision after metadata request`() = runBlocking {
        val disk = MemoryStorage(initial).apply {
            auto = true
            lastSyncedHash = syncContentHash(initial)
        }
        TestServer { request ->
            if (request.path == "/api/1/configs") {
                disk.saveYaml(edited)
                Response(200, "[${remote(initial)}]")
            } else Response(200, remote(initial))
        }.use { server ->
            // Configure only the fake endpoint in the synthetic local document.
            val configured = initial.replace("https://sync.example.test", server.host)
            val newEdit = configured.replace("name: original", "name: edited-on-phone")
            disk.saveYaml(configured)
            disk.lastSyncedHash = syncContentHash(configured)
            server.handler = { request ->
                if (request.path == "/api/1/configs") {
                    disk.saveYaml(newEdit)
                    Response(200, "[${remote(initial)}]")
                } else Response(200, remote(initial))
            }
            runCatching { repository(disk).autoSyncTick() }
            assertEquals(newEdit, disk.loadYaml())
            assertEquals(syncContentHash(configured), disk.lastSyncedHash)
        }
    }

    @Test fun `persisted conflict pauses every automatic transfer direction`() = runBlocking {
        for ((localChanged, remoteChanged) in listOf(true to false, false to true, true to true)) {
            val disk = MemoryStorage(initial).apply { auto = true; syncConflict = true }
            val remoteStamp = if (remoteChanged) "remote-new" else "baseline-stamp"
            TestServer { request ->
                val meta = remote(initial, remoteStamp)
                Response(200, if (request.path == "/api/1/configs") "[$meta]" else meta)
            }.use { server ->
                val configured = initial.replace("https://sync.example.test", server.host)
                disk.saveYaml(configured)
                disk.lastSyncedHash = syncContentHash(
                    if (localChanged) configured.replace("name: original", "name: older-baseline") else configured,
                )
                val baselineHash = disk.lastSyncedHash

                assertEquals(id.web.izs.sshclient.core.sync.AutoSyncOutcome.Clean, repository(disk).autoSyncTick())
                assertTrue("Paused sync must not contact the server", server.requests.isEmpty())
                assertEquals(configured, disk.loadYaml())
                assertEquals(baselineHash, disk.lastSyncedHash)
                assertEquals("baseline-stamp", disk.lastRemoteChange)
                assertTrue(disk.syncConflict)
            }
        }
    }

    @Test fun `new conflict is reported once and following ticks remain paused`() = runBlocking {
        val disk = MemoryStorage(initial).apply { auto = true }
        TestServer { Response(200, "[${remote(initial)}]") }.use { server ->
            val configured = initial.replace("https://sync.example.test", server.host)
            disk.saveYaml(configured)
            val repo = repository(disk)
            assertEquals(id.web.izs.sshclient.core.sync.AutoSyncOutcome.Conflict, repo.autoSyncTick())
            assertTrue(disk.syncConflict)
            val requestsAtConflict = server.requests.size
            assertEquals(1, requestsAtConflict)

            assertEquals(id.web.izs.sshclient.core.sync.AutoSyncOutcome.Clean, repo.autoSyncTick())
            assertEquals(requestsAtConflict, server.requests.size)
            assertEquals(configured, disk.loadYaml())
            assertEquals("baseline-hash", disk.lastSyncedHash)
            assertTrue(disk.syncConflict)
        }
    }

    @Test fun `explicit upload resolves paused conflict and automatic checks can resume`() = runBlocking {
        val disk = MemoryStorage(initial).apply { auto = true; syncConflict = true }
        var uploaded = ""
        TestServer { request ->
            when {
                request.method == "PATCH" -> {
                    uploaded = uploadedContent(request)
                    Response(204)
                }
                request.path == "/api/1/configs" -> Response(200, "[${remote(uploaded)}]")
                else -> Response(200, remote(uploaded.ifBlank { initial }))
            }
        }.use { server ->
            disk.saveYaml(initial.replace("https://sync.example.test", server.host))
            val repo = repository(disk)
            repo.uploadAsCurrent(server.host, "fake-token", 7, "test")
            assertFalse(disk.syncConflict)
            val requestsAfterResolution = server.requests.size

            assertEquals(id.web.izs.sshclient.core.sync.AutoSyncOutcome.Clean, repo.autoSyncTick())
            assertEquals(requestsAfterResolution + 1, server.requests.size)
            assertEquals("/api/1/configs", server.requests.last().path)
            assertEquals(1, server.requests.count { it.method == "PATCH" })
        }
    }

    @Test fun `explicit download resolves paused conflict and automatic checks can resume`() = runBlocking {
        val disk = MemoryStorage(initial).apply { auto = true; syncConflict = true }
        TestServer { request ->
            val meta = remote(edited)
            Response(200, if (request.path == "/api/1/configs") "[$meta]" else meta)
        }.use { server ->
            disk.saveYaml(initial.replace("https://sync.example.test", server.host))
            val repo = repository(disk)
            repo.downloadIntoLocal(server.host, "fake-token", 7)
            assertFalse(disk.syncConflict)
            val requestsAfterResolution = server.requests.size

            assertEquals(id.web.izs.sshclient.core.sync.AutoSyncOutcome.Clean, repo.autoSyncTick())
            assertEquals(requestsAfterResolution + 1, server.requests.size)
            assertEquals("/api/1/configs", server.requests.last().path)
            assertFalse(server.requests.any { it.method == "PATCH" })
        }
    }

    @Test fun `successful upload preserves raw unknown fields and absent auth`() = runBlocking {
        val disk = MemoryStorage(initial)
        var uploaded = ""
        TestServer { request ->
            if (request.method == "PATCH") {
                uploaded = uploadedContent(request)
                Response(204)
            } else Response(200, remote(uploaded.ifBlank { "{}" }, id = 8))
        }.use { server ->
            repository(disk).uploadAsCurrent(server.host, "fake-token", 8, "test")
            val raw = RawConfigStore.loadRaw(uploaded)
            assertTrue(raw.containsKey("futureExtension"))
            assertFalse(raw.containsKey("configSync"))
            assertFalse(uploaded.contains("auth:"))
            assertEquals(8L, RawConfigStore.syncTargetOf(RawConfigStore.loadRaw(disk.loadYaml()!!)).configId)
            assertEquals("remote-new", disk.lastRemoteChange)
            assertEquals(syncContentHash(disk.loadYaml()!!), disk.lastSyncedHash)
        }
    }

    @Test fun `auto upload stops when remote changes between metadata and merge read`() = runBlocking {
        val disk = MemoryStorage(initial).apply { auto = true }
        TestServer { request ->
            when {
                request.path == "/api/1/configs" -> Response(200, "[${remote(initial, "baseline-stamp")}]")
                request.method == "GET" -> Response(200, remote(edited, "another-pc-change"))
                else -> Response(204)
            }
        }.use { server ->
            val configured = initial.replace("https://sync.example.test", server.host)
            disk.saveYaml(configured)
            assertEquals(id.web.izs.sshclient.core.sync.AutoSyncOutcome.Conflict, repository(disk).autoSyncTick())
            assertEquals(0, server.requests.count { it.method == "PATCH" })
            assertEquals(configured, disk.loadYaml())
            assertEquals("baseline-stamp", disk.lastRemoteChange)
            assertTrue(disk.syncConflict)
        }
    }

    @Test fun `download accepts desktop encrypted vault and keeps it locked until unlock`() = runBlocking {
        val fixture = requireNotNull(javaClass.classLoader).getResourceAsStream("desktop-vault.json")!!
            .bufferedReader().use { Json.parseToJsonElement(it.readText()).jsonObject }
        val envelope = RawConfigStore.jsonToRaw(fixture.getValue("stored").toString())
        val content = RawConfigStore.dumpRaw(linkedMapOf("encrypted" to true, "vault" to envelope))
        val disk = MemoryStorage(initial)
        val repository = repository(disk)
        TestServer { Response(200, remote(content)) }.use { server ->
            val downloaded = repository.downloadIntoLocal(server.host, "fake-token", 7)
            assertTrue(downloaded.needsPassphrase)
            val saved = RawConfigStore.loadRaw(disk.loadYaml()!!)
            assertEquals(
                RawConfigStore.storedVault(mapOf("vault" to envelope)),
                RawConfigStore.storedVault(saved),
            )
            val unlocked = repository.unlockWithPassphrase(fixture.getValue("passphrase").jsonPrimitive.content)
            assertEquals(4, unlocked.domain.profiles.size)
        }
    }

    @Test fun `empty mapping and settings only mapping are valid downloads`() = runBlocking {
        for (content in listOf("{}", "appearance: {theme: remote-theme}")) {
            val disk = MemoryStorage(initial)
            TestServer { Response(200, remote(content)) }.use { server ->
                repository(disk).downloadIntoLocal(server.host, "fake-token", 7)
                assertEquals(initial, disk.loadPreImportYaml())
                assertEquals("remote-new", disk.lastRemoteChange)
                assertEquals(syncContentHash(disk.loadYaml()!!), disk.lastSyncedHash)
            }
        }
    }

    private fun uploadedContent(request: Request): String =
        Json.parseToJsonElement(request.body).jsonObject.getValue("content").jsonPrimitive.content

    private fun repository(disk: MemoryStorage) = SyncRepository(
        disk,
        TabbySyncApi(OkHttpClient.Builder().callTimeout(5, TimeUnit.SECONDS).build()),
    )

    private data class Request(val method: String, val path: String, val body: String)
    private data class Response(val code: Int, val body: String = "")

    private class TestServer(@Volatile var handler: (Request) -> Response) : AutoCloseable {
        val requests: MutableList<Request> = Collections.synchronizedList(mutableListOf())
        private val executor = Executors.newCachedThreadPool()
        private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            this.executor = this@TestServer.executor
            createContext("/") { exchange ->
                val request = Request(exchange.requestMethod, exchange.requestURI.path, exchange.requestBody.bufferedReader().readText())
                requests.add(request)
                val response = handler(request)
                val bytes = response.body.toByteArray(Charsets.UTF_8)
                exchange.responseHeaders.set("Content-Type", "application/json")
                exchange.sendResponseHeaders(response.code, if (response.code == 204) -1 else bytes.size.toLong())
                exchange.responseBody.use { if (response.code != 204) it.write(bytes) }
                exchange.close()
            }
            start()
        }
        val host: String get() = "http://127.0.0.1:${server.address.port}"
        override fun close() { server.stop(0); executor.shutdownNow() }
    }

    private class MemoryStorage(private var yaml: String?) : SyncStorage {
        @Volatile override var auto = false
        @Volatile override var partsHotkeys = true
        @Volatile override var partsAppearance = true
        @Volatile override var partsVault = true
        @Volatile override var lastRemoteChange = "baseline-stamp"
        @Volatile override var lastSyncedHash = "baseline-hash"
        @Volatile override var syncConflict = false
        private var backup: String? = null
        private var backupStamp = ""
        override fun <T> withSyncStateLock(block: () -> T): T = synchronized(this, block)
        @Synchronized override fun loadYaml(): String? = yaml
        @Synchronized override fun saveYaml(yaml: String) { this.yaml = yaml }
        override fun dropLegacySyncTarget() = Unit
        override fun loadKnownHostsJson(): String? = null
        override fun saveKnownHostsJson(json: String) = Unit
        override fun savePreImport(yaml: String, lastChange: String) { backup = yaml; backupStamp = lastChange }
        override fun loadPreImportYaml(): String? = backup
        override fun loadPreImportStamp(): String = backupStamp
    }
}
