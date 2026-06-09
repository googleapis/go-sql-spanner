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

export type ResourceCleanupInfo =
  | { type: 'pool'; poolId: number }
  | { type: 'connection'; poolId: number; connectionId: number }
  | { type: 'rows'; poolId: number; connectionId: number; rowsId: number };

export class SpannerLib {
  private registry: FinalizationRegistry<ResourceCleanupInfo>;

  constructor() {
    this.registry = new FinalizationRegistry((info: ResourceCleanupInfo) => {
      switch (info.type) {
        case 'pool':
          ffi.invokeAsync('ClosePool', info.poolId).catch(() => {});
          break;
        case 'connection':
          ffi
            .invokeAsync('CloseConnection', info.poolId, info.connectionId)
            .catch(() => {});
          break;
        case 'rows':
          ffi
            .invokeAsync(
              'CloseRows',
              info.poolId,
              info.connectionId,
              info.rowsId
            )
            .catch(() => {});
          break;
      }
    });
  }

  register(refInstance: object, info: ResourceCleanupInfo): void {
    this.registry.register(refInstance, info, refInstance);
  }

  unregister(refInstance: object): void {
    this.registry.unregister(refInstance);
  }
}

export const spannerLib = new SpannerLib();
