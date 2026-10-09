import * as NodeSqlite from "node:sqlite";

import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

export class V2DatabaseImportError extends Schema.TaggedError<V2DatabaseImportError>()(
  "V2DatabaseImportError",
  { sourcePath: Schema.String, destinationPath: Schema.String, cause: Schema.Defect() },
) {
  override get message() {
    return `Could not copy the V1 database at ${this.sourcePath} to ${this.destinationPath}. The V1 database has not been migrated.`;
  }
}

/**
 * Read the highest V1 event sequence recorded in a database. V2 tags its own
 * events with `application_event_version = 2`, so a migrated V2 database only
 * reports the V1-origin high water; a raw pre-migration copy has no such column
 * and every event is V1-origin. Returns null when the layout is unexpected so
 * callers skip the comparison instead of failing startup.
 */
const readV1EventSequenceHighWater = Effect.fn("readV1EventSequenceHighWater")(function* (
  databasePath: string,
) {
  return yield* Effect.try(() => {
    const database = new NodeSqlite.DatabaseSync(databasePath, { readOnly: true });
    try {
      const table = database
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'orchestration_events'",
        )
        .get();
      if (!table) return null;
      const versionColumn = database
        .prepare(
          "SELECT name FROM pragma_table_info('orchestration_events') WHERE name = 'application_event_version'",
        )
        .get();
      const row = database
        .prepare(
          versionColumn
            ? "SELECT MAX(sequence) AS highWater FROM orchestration_events WHERE application_event_version = 1"
            : "SELECT MAX(sequence) AS highWater FROM orchestration_events",
        )
        .get() as { readonly highWater: number | null } | undefined;
      return row?.highWater ?? null;
    } finally {
      database.close();
    }
  }).pipe(Effect.orElseSucceed((): number | null => null));
});

/** Seed V2 once. Its copied legacy tables remain the source for lazy transcript import. */
export const initializeV2Database = Effect.fn("initializeV2Database")(function* (
  destinationPath: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = path.dirname(destinationPath);
  const sourcePath = path.join(directory, "state.sqlite");
  if (yield* fs.exists(destinationPath)) {
    // A preview or an earlier V2 start can leave a database that predates the
    // current V1 state. V2 never re-reads V1 once this file exists, so warn
    // instead of silently omitting newer V1 threads. Both files stay untouched.
    if (yield* fs.exists(sourcePath)) {
      const sourceHighWater = yield* readV1EventSequenceHighWater(sourcePath);
      const destinationHighWater = yield* readV1EventSequenceHighWater(destinationPath);
      if (
        sourceHighWater !== null &&
        destinationHighWater !== null &&
        sourceHighWater > destinationHighWater
      ) {
        yield* Effect.logWarning(
          "statev2.sqlite already exists and holds an older copy of state.sqlite than the current one. V1 threads and changes made since that copy will not be imported. To migrate the current V1 data instead, stop T3 Code, move statev2.sqlite (and its -wal and -shm files) aside, and restart. Threads created only in V2 will not be in the new copy, so keep the moved files if you need them.",
          { sourcePath, destinationPath, sourceHighWater, destinationHighWater },
        );
      }
    }
    return;
  }
  if (!(yield* fs.exists(sourcePath))) return;
  yield* Effect.gen(function* () {
    const temporaryDirectory = yield* fs.makeTempDirectoryScoped({
      directory,
      prefix: ".v2-import-",
    });
    const snapshotPath = path.join(temporaryDirectory, "snapshot.sqlite");
    yield* Effect.tryPromise(async () => {
      const database = new NodeSqlite.DatabaseSync(sourcePath, { readOnly: true });
      try {
        await NodeSqlite.backup(database, snapshotPath);
      } finally {
        database.close();
      }
    });
    // Publish only a complete snapshot, without replacing an existing V2 database.
    yield* fs
      .link(snapshotPath, destinationPath)
      .pipe(
        Effect.catch((error) =>
          error.reason._tag === "AlreadyExists" ? Effect.void : Effect.fail(error),
        ),
      );
  }).pipe(
    Effect.scoped,
    Effect.mapError((cause) => new V2DatabaseImportError({ sourcePath, destinationPath, cause })),
  );
});
