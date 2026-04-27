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
