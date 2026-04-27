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

import { createRequire } from 'module';
// @ts-ignore
const _require = typeof require !== 'undefined' ? require : createRequire(import.meta.url);
const addon = _require('../../../Release/spanner_napi.node');

export const ENCODING_JSON = 0;
export const ENCODING_PROTOBUF = 1;

export interface HandledResult {
    objectId: number;
    pinnerId: number;
    protobufBytes: Buffer | null;
}

export function invokeAsync(funcName: string, constructor1: any, constructor2: any, ...args: any[]): Promise<HandledResult> {
    return new Promise((resolve, reject) => {
        const callback = (err: any, result: any) => {
            if (err) {
                return reject(err);
            }
            if (result.r1 !== 0) {
                if (result.r4 && result.r3 > 0) {
                    const errorJson = result.r4.toString('utf8');
                    try {
                        const parsed = JSON.parse(errorJson);
                        return reject(new Error(parsed.message || errorJson));
                    } catch (e) {
                        return reject(new Error(errorJson));
                    }
                }
                return reject(new Error(`Native Spanner Error Code: ${result.r1}`));
            }

            resolve({
                objectId: result.r2,
                pinnerId: result.r0,
                protobufBytes: result.r4
            });
        };

        addon[funcName](...args, callback);
    });
}

export const Release = addon.Release;
export class SpannerLibError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'SpannerLibError';
    }
}
