package chrondb.lib;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.WordFactory;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * C entry points for the ChronDB shared library.
 * All methods are static and annotated with @CEntryPoint for GraalVM native-image --shared.
 *
 * String return values must be freed by the caller using chrondb_free_string.
 * NULL branch means default branch.
 */
public final class ChronDBLib {

    private static volatile String lastError = null;

    private static volatile boolean initialized = false;
    private static IFn libOpen;
    private static IFn libClose;
    private static IFn libPut;
    private static IFn libGet;
    private static IFn libDelete;
    private static IFn libListByPrefix;
    private static IFn libListByTable;
    private static IFn libHistory;
    private static IFn libQuery;

    private static synchronized void ensureInitialized() {
        if (!initialized) {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read("chrondb.lib.core"));

            libOpen = Clojure.var("chrondb.lib.core", "lib-open");
            libClose = Clojure.var("chrondb.lib.core", "lib-close");
            libPut = Clojure.var("chrondb.lib.core", "lib-put");
            libGet = Clojure.var("chrondb.lib.core", "lib-get");
            libDelete = Clojure.var("chrondb.lib.core", "lib-delete");
            libListByPrefix = Clojure.var("chrondb.lib.core", "lib-list-by-prefix");
            libListByTable = Clojure.var("chrondb.lib.core", "lib-list-by-table");
            libHistory = Clojure.var("chrondb.lib.core", "lib-history");
            libQuery = Clojure.var("chrondb.lib.core", "lib-query");

            initialized = true;
        }
    }

    private static CCharPointer toCString(String s) {
        if (s == null) {
            return WordFactory.nullPointer();
        }
        return CTypeConversion.toCString(s).get();
    }

    private static String toJavaString(CCharPointer ptr) {
        if (ptr.isNull()) {
            return null;
        }
        return CTypeConversion.toJavaString(ptr);
    }

    // --- Lifecycle ---

    @CEntryPoint(name = "chrondb_open")
    public static int open(IsolateThread thread, CCharPointer dataPath, CCharPointer indexPath) {
        try {
            ensureInitialized();
            String dp = toJavaString(dataPath);
            String ip = toJavaString(indexPath);
            Object result = libOpen.invoke(dp, ip);
            if (result instanceof Number) {
                int handle = ((Number) result).intValue();
                if (handle < 0) {
                    // Clojure returned -1 (error) - provide a meaningful error message
                    lastError = "Failed to open database at " + dp + " (index: " + ip + "). " +
                        "Check that the paths are valid and writable.";
                }
                return handle;
            }
            lastError = ("open returned non-numeric result: " +
                (result == null ? "null" : result.getClass().getName()));
            return -1;
        } catch (Exception e) {
            String msg = e.getMessage();
            lastError = (e.getClass().getName() + ": " + (msg != null ? msg : "no message"));
            return -1;
        }
    }

    @CEntryPoint(name = "chrondb_close")
    public static int close(IsolateThread thread, int handle) {
        try {
            ensureInitialized();
            Object result = libClose.invoke(handle);
            if (result instanceof Number) {
                return ((Number) result).intValue();
            }
            return -1;
        } catch (Exception e) {
            lastError = (e.getMessage());
            return -1;
        }
    }

    // --- Storage ---

    @CEntryPoint(name = "chrondb_put")
    public static CCharPointer put(IsolateThread thread, int handle,
                                   CCharPointer id, CCharPointer jsonDoc, CCharPointer branch) {
        try {
            ensureInitialized();
            String idStr = toJavaString(id);
            String jsonStr = toJavaString(jsonDoc);
            String branchStr = toJavaString(branch);
            Object result = libPut.invoke(handle, idStr, jsonStr, branchStr);
            if (result instanceof String) {
                return toCString((String) result);
            }
            lastError = ("put returned null");
            return WordFactory.nullPointer();
        } catch (Exception e) {
            lastError = (e.getMessage());
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "chrondb_get")
    public static CCharPointer get(IsolateThread thread, int handle,
                                   CCharPointer id, CCharPointer branch) {
        try {
            ensureInitialized();
            String idStr = toJavaString(id);
            String branchStr = toJavaString(branch);
            Object result = libGet.invoke(handle, idStr, branchStr);
            if (result instanceof String) {
                return toCString((String) result);
            }
            return WordFactory.nullPointer();
        } catch (Exception e) {
            lastError = (e.getMessage());
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "chrondb_delete")
    public static int delete(IsolateThread thread, int handle,
                             CCharPointer id, CCharPointer branch) {
        try {
            ensureInitialized();
            String idStr = toJavaString(id);
            String branchStr = toJavaString(branch);
            Object result = libDelete.invoke(handle, idStr, branchStr);
            if (result instanceof Number) {
                return ((Number) result).intValue();
            }
            return -1;
        } catch (Exception e) {
            lastError = (e.getMessage());
            return -1;
        }
    }

    @CEntryPoint(name = "chrondb_list_by_prefix")
    public static CCharPointer listByPrefix(IsolateThread thread, int handle,
                                            CCharPointer prefix, CCharPointer branch) {
        try {
            ensureInitialized();
            String prefixStr = toJavaString(prefix);
            String branchStr = toJavaString(branch);
            Object result = libListByPrefix.invoke(handle, prefixStr, branchStr);
            if (result instanceof String) {
                return toCString((String) result);
            }
            return WordFactory.nullPointer();
        } catch (Exception e) {
            lastError = (e.getMessage());
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "chrondb_list_by_table")
    public static CCharPointer listByTable(IsolateThread thread, int handle,
                                           CCharPointer table, CCharPointer branch) {
        try {
            ensureInitialized();
            String tableStr = toJavaString(table);
            String branchStr = toJavaString(branch);
            Object result = libListByTable.invoke(handle, tableStr, branchStr);
            if (result instanceof String) {
                return toCString((String) result);
            }
            return WordFactory.nullPointer();
        } catch (Exception e) {
            lastError = (e.getMessage());
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "chrondb_history")
    public static CCharPointer history(IsolateThread thread, int handle,
                                       CCharPointer id, CCharPointer branch) {
        try {
            ensureInitialized();
            String idStr = toJavaString(id);
            String branchStr = toJavaString(branch);
            Object result = libHistory.invoke(handle, idStr, branchStr);
            if (result instanceof String) {
                return toCString((String) result);
            }
            return WordFactory.nullPointer();
        } catch (Exception e) {
            lastError = (e.getMessage());
            return WordFactory.nullPointer();
        }
    }

    // --- Query ---

    @CEntryPoint(name = "chrondb_query")
    public static CCharPointer query(IsolateThread thread, int handle,
                                     CCharPointer queryJson, CCharPointer branch) {
        try {
            ensureInitialized();
            String queryStr = toJavaString(queryJson);
            String branchStr = toJavaString(branch);
            Object result = libQuery.invoke(handle, queryStr, branchStr);
            if (result instanceof String) {
                return toCString((String) result);
            }
            return WordFactory.nullPointer();
        } catch (Exception e) {
            lastError = (e.getMessage());
            return WordFactory.nullPointer();
        }
    }

    // --- Utilities ---

    @CEntryPoint(name = "chrondb_free_string")
    public static void freeString(IsolateThread thread, CCharPointer ptr) {
        // GraalVM manages CCharPointer memory through CTypeConversion pinning.
        // In practice, strings returned via toCString().get() are pinned and
        // freed when the CCharPointerHolder is closed. For the shared library
        // pattern, we rely on GraalVM's UnmanagedMemory or the caller to manage
        // the lifecycle. This is a no-op placeholder for API completeness;
        // actual memory strategy depends on the GraalVM version used.
    }

    @CEntryPoint(name = "chrondb_last_error")
    public static CCharPointer getLastError(IsolateThread thread) {
        String error = lastError;
        if (error != null) {
            lastError = null;
            return toCString(error);
        }
        return WordFactory.nullPointer();
    }
}
