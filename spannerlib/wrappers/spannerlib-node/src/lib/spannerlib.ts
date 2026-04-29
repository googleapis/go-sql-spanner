// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { ffi } from '../ffi/utils.js';

export class SpannerLib {
  private activePinners: Set<number>;
  private registry: FinalizationRegistry<number>;

  constructor() {
    this.activePinners = new Set();

    this.registry = new FinalizationRegistry((pinnerId: number) => {
      if (pinnerId && pinnerId > 0) {
        ffi.Release(pinnerId);
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
      ffi.Release(pinnerId);
      this.registry.unregister(refInstance);
      this.activePinners.delete(pinnerId);
    }
  }

  releaseAll(): void {
    for (const pinnerId of this.activePinners) {
      ffi.Release(pinnerId);
    }
    this.activePinners.clear();
  }
}

export const spannerLib = new SpannerLib();
