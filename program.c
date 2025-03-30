// This program demonstrates thread-safe add and get in a simplified LSM-tree (memtable) using pthread read-write locks.
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>   // for sleep and usleep
#include <string.h>   // for memset

// Data structure for a memtable entry (key-value pair).
typedef struct {
    int key;
    int value;
    int in_use; // flag indicating if this entry is used (1) or empty (0)
} Entry;

// LSM tree structure containing an in-memory memtable and an RW-lock for synchronization.
typedef struct {
    Entry *memtable;          // dynamic array of entries
    int memtable_max;         // maximum number of entries memtable can hold
    int memtable_count;       // current count of entries in memtable
    pthread_rwlock_t rwlock;  // read-write lock for protecting the memtable
} LSMTree;

LSMTree lsm; // Global LSMTree instance for this demonstration

// Initialize the LSM tree (allocate memtable and initialize the lock).
void lsm_init(LSMTree *tree, int max_entries) {
    tree->memtable_max = max_entries;
    tree->memtable_count = 0;
    // Allocate memory for memtable entries and mark all as empty
    tree->memtable = malloc(sizeof(Entry) * max_entries);
    if (tree->memtable == NULL) {
        fprintf(stderr, "Memtable allocation failed\n");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < max_entries; ++i) {
        tree->memtable[i].in_use = 0;
    }
    // Initialize the read-write lock with default attributes
    pthread_rwlock_init(&tree->rwlock, NULL);
}

// Destroy the LSM tree (free memtable and destroy the lock).
void lsm_destroy(LSMTree *tree) {
    pthread_rwlock_destroy(&tree->rwlock);
    free(tree->memtable);
    tree->memtable = NULL;
    tree->memtable_max = 0;
    tree->memtable_count = 0;
}

// Thread-safe add (write) operation: insert or update a key-value pair.
void lsm_add(LSMTree *tree, int key, int value) {
    // Acquire write lock for exclusive access
    pthread_rwlock_wrlock(&tree->rwlock);
    // Check if key already exists; if so, update it
    int updated = 0;
    for (int i = 0; i < tree->memtable_count; ++i) {
        if (tree->memtable[i].in_use && tree->memtable[i].key == key) {
            tree->memtable[i].value = value;
            updated = 1;
            break;
        }
    }
    // If new key and space is available, add the entry
    if (!updated) {
        if (tree->memtable_count < tree->memtable_max) {
            tree->memtable[tree->memtable_count].key = key;
            tree->memtable[tree->memtable_count].value = value;
            tree->memtable[tree->memtable_count].in_use = 1;
            tree->memtable_count++;
        } else {
            // If memtable is full, in a real LSM-tree we'd flush to disk.
            fprintf(stderr, "Memtable full, cannot add key %d (would trigger flush)\n", key);
        }
    }
    printf("Writer: added key %d, value %d\n", key, value);
    // Release the write lock
    pthread_rwlock_unlock(&tree->rwlock);
}

// Thread-safe get (read) operation: retrieve the value for a given key.
// Returns 1 if found (and *out_value is set), or 0 if not found.
int lsm_get(LSMTree *tree, int key, int *out_value) {
    // Acquire read lock for shared access
    pthread_rwlock_rdlock(&tree->rwlock);
    int found = 0;
    for (int i = 0; i < tree->memtable_count; ++i) {
        if (tree->memtable[i].in_use && tree->memtable[i].key == key) {
            *out_value = tree->memtable[i].value;
            found = 1;
            break;
        }
    }
    // Release the read lock after finishing the search
    pthread_rwlock_unlock(&tree->rwlock);
    return found;
}

// Structure for passing thread parameters (like thread id and number of operations).
typedef struct {
    int id;
    int operations;
} ThreadArg;

// Writer thread function: adds a sequence of key-value pairs.
void* writer_thread(void *arg) {
    ThreadArg *targ = (ThreadArg*) arg;
    int id = targ->id;
    int ops = targ->operations;
    for (int i = 0; i < ops; ++i) {
        int key = id * 100 + i;   // generate unique key based on thread id
        int value = key * 10;     // sample value (10x the key for demonstration)
        lsm_add(&lsm, key, value);
        // Sleep briefly to simulate work and encourage context switching
        usleep(100000); // 0.1 seconds
    }
    printf("Writer thread %d finished\n", id);
    return NULL;
}

// Reader thread function: repeatedly reads a range of keys.
void* reader_thread(void *arg) {
    ThreadArg *targ = (ThreadArg*) arg;
    int id = targ->id;
    int ops = targ->operations;
    // This reader will attempt to read keys in the range [100, 100+ops) and [200, 200+ops).
    for (int i = 0; i < ops; ++i) {
        int key1 = 100 + i;
        int key2 = 200 + i;
        int value;
        if (lsm_get(&lsm, key1, &value)) {
            printf("Reader %d: got key %d -> value %d\n", id, key1, value);
        } else {
            printf("Reader %d: key %d not found\n", id, key1);
        }
        if (lsm_get(&lsm, key2, &value)) {
            printf("Reader %d: got key %d -> value %d\n", id, key2, value);
        } else {
            printf("Reader %d: key %d not found\n", id, key2);
        }
        // Sleep briefly to simulate processing
        usleep(150000); // 0.15 seconds
    }
    printf("Reader thread %d finished\n", id);
    return NULL;
}

// Main function: set up the LSM tree and spawn multiple readers/writers to demonstrate concurrent access.
int main() {
    // Initialize LSM tree with capacity for 50 entries
    lsm_init(&lsm, 50);

    // Set number of operations for writer and reader threads
    int writer_ops = 10;
    int reader_ops = 10;
    // Prepare thread arguments
    ThreadArg writer_args[2];
    ThreadArg reader_args[2];
    pthread_t writer_threads[2];
    pthread_t reader_threads[2];

    // Create writer threads (2 writers)
    // (pthread_create return values not checked for brevity)
    for (int i = 0; i < 2; ++i) {
        writer_args[i].id = i + 1;
        writer_args[i].operations = writer_ops;
        pthread_create(&writer_threads[i], NULL, writer_thread, &writer_args[i]);
    }
    // Create reader threads (2 readers)
    // (pthread_create return values not checked for brevity)
    for (int j = 0; j < 2; ++j) {
        reader_args[j].id = j + 1;
        reader_args[j].operations = reader_ops;
        pthread_create(&reader_threads[j], NULL, reader_thread, &reader_args[j]);
    }

    // Wait for all threads to complete
    // (pthread_join return values not checked for brevity)
    for (int i = 0; i < 2; ++i) {
        pthread_join(writer_threads[i], NULL);
    }
    for (int j = 0; j < 2; ++j) {
        pthread_join(reader_threads[j], NULL);
    }

    // After threads complete, print the final memtable contents
    printf("Final memtable contents (%d entries):\n", lsm.memtable_count);
    for (int i = 0; i < lsm.memtable_count; ++i) {
        if (lsm.memtable[i].in_use) {
            printf("  key %d -> value %d\n", lsm.memtable[i].key, lsm.memtable[i].value);
        }
    }

    // Clean up resources
    lsm_destroy(&lsm);
    return 0;
}
