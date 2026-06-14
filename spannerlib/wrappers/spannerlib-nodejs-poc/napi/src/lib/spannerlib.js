const { Release } = require('../ffi/utils.js');

class SpannerLib {
    constructor() {
        this.activePinners = new Set();

        // FinalizationRegistry automatically guarantees that V8 Garbage Collection
        // dropping an object triggers this callback, where we gracefully
        // wipe the matching Go pointer from memory via `Release(id)`.
        this.registry = new FinalizationRegistry((pinnerId) => {
            if (pinnerId && pinnerId > 0) {
                Release(pinnerId);
                this.activePinners.delete(pinnerId);
            }
        });
    }

    /**
     * Registers a JavaScript object with this Memory Tracker.
     * When JS garbage collects `refInstance`, `pinnerId` is automatically natively released.
     */
    register(refInstance, pinnerId) {
        if (pinnerId > 0) {
            this.activePinners.add(pinnerId);
            this.registry.register(refInstance, pinnerId, refInstance);
        }
    }

    unregister(refInstance, pinnerId) {
        if (pinnerId > 0) {
            Release(pinnerId);
            this.registry.unregister(refInstance);
            this.activePinners.delete(pinnerId);
        }
    }

    /**
     * Fallback shutdown function to crash-stop everything not collected yet.
     */
    releaseAll() {
        for (const pinnerId of this.activePinners) {
            Release(pinnerId);
        }
        this.activePinners.clear();
    }
}

// Global Singleton
const spannerLib = new SpannerLib();

module.exports = { spannerLib };
