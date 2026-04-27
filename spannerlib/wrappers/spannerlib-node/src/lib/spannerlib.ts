import { Release } from '../ffi/utils.js';

export class SpannerLib {
    private activePinners: Set<number>;
    private registry: FinalizationRegistry<any>;

    constructor() {
        this.activePinners = new Set();

        this.registry = new FinalizationRegistry((pinnerId: number) => {
            if (pinnerId && pinnerId > 0) {
                Release(pinnerId);
                this.activePinners.delete(pinnerId);
            }
        });
    }

    register(refInstance: object, pinnerId: number): void {
        if (pinnerId > 0) {
            this.activePinners.add(pinnerId);
            this.registry.register(refInstance, pinnerId, refInstance);
        }
    }

    unregister(refInstance: object, pinnerId: number): void {
        if (pinnerId > 0) {
            Release(pinnerId);
            this.registry.unregister(refInstance);
            this.activePinners.delete(pinnerId);
        }
    }

    releaseAll(): void {
        for (const pinnerId of this.activePinners) {
            Release(pinnerId);
        }
        this.activePinners.clear();
    }
}

export const spannerLib = new SpannerLib();
