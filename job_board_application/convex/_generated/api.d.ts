/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as __tests___getHandler from "../__tests__/getHandler.js";
import type * as admin from "../admin.js";
import type * as auth from "../auth.js";
import type * as companySummaryCron from "../companySummaryCron.js";
import type * as crons from "../crons.js";
import type * as filters from "../filters.js";
import type * as firecrawlWebhookUtil from "../firecrawlWebhookUtil.js";
import type * as formFiller from "../formFiller.js";
import type * as http from "../http.js";
import type * as jobRecords from "../jobRecords.js";
import type * as jobs from "../jobs.js";
import type * as location from "../location.js";
import type * as middleware_firecrawlCors from "../middleware/firecrawlCors.js";
import type * as migrations from "../migrations.js";
import type * as router from "../router.js";
import type * as scratchpad from "../scratchpad.js";
import type * as seedData from "../seedData.js";
import type * as siteScheduleSync from "../siteScheduleSync.js";
import type * as siteTypes from "../siteTypes.js";
import type * as siteUtils from "../siteUtils.js";
import type * as sites from "../sites.js";
import type * as temporal from "../temporal.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

declare const fullApi: ApiFromModules<{
  "__tests__/getHandler": typeof __tests___getHandler;
  admin: typeof admin;
  auth: typeof auth;
  companySummaryCron: typeof companySummaryCron;
  crons: typeof crons;
  filters: typeof filters;
  firecrawlWebhookUtil: typeof firecrawlWebhookUtil;
  formFiller: typeof formFiller;
  http: typeof http;
  jobRecords: typeof jobRecords;
  jobs: typeof jobs;
  location: typeof location;
  "middleware/firecrawlCors": typeof middleware_firecrawlCors;
  migrations: typeof migrations;
  router: typeof router;
  scratchpad: typeof scratchpad;
  seedData: typeof seedData;
  siteScheduleSync: typeof siteScheduleSync;
  siteTypes: typeof siteTypes;
  siteUtils: typeof siteUtils;
  sites: typeof sites;
  temporal: typeof temporal;
}>;

/**
 * A utility for referencing Convex functions in your app's public API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
export declare const api: FilterApi<
  typeof fullApi,
  FunctionReference<any, "public">
>;

/**
 * A utility for referencing Convex functions in your app's internal API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = internal.myModule.myFunction;
 * ```
 */
export declare const internal: FilterApi<
  typeof fullApi,
  FunctionReference<any, "internal">
>;

export declare const components: {
  crons: {
    public: {
      del: FunctionReference<
        "mutation",
        "internal",
        { identifier: { id: string } | { name: string } },
        null
      >;
      get: FunctionReference<
        "query",
        "internal",
        { identifier: { id: string } | { name: string } },
        {
          args: Record<string, any>;
          functionHandle: string;
          id: string;
          name?: string;
          schedule:
            | { kind: "interval"; ms: number }
            | { cronspec: string; kind: "cron"; tz?: string };
        } | null
      >;
      list: FunctionReference<
        "query",
        "internal",
        {},
        Array<{
          args: Record<string, any>;
          functionHandle: string;
          id: string;
          name?: string;
          schedule:
            | { kind: "interval"; ms: number }
            | { cronspec: string; kind: "cron"; tz?: string };
        }>
      >;
      register: FunctionReference<
        "mutation",
        "internal",
        {
          args: Record<string, any>;
          functionHandle: string;
          name?: string;
          schedule:
            | { kind: "interval"; ms: number }
            | { cronspec: string; kind: "cron"; tz?: string };
        },
        string
      >;
    };
  };
  migrations: {
    lib: {
      cancel: FunctionReference<
        "mutation",
        "internal",
        { name: string },
        {
          batchSize?: number;
          cursor?: string | null;
          error?: string;
          isDone: boolean;
          latestEnd?: number;
          latestStart: number;
          name: string;
          next?: Array<string>;
          processed: number;
          state: "inProgress" | "success" | "failed" | "canceled" | "unknown";
        }
      >;
      cancelAll: FunctionReference<
        "mutation",
        "internal",
        { sinceTs?: number },
        Array<{
          batchSize?: number;
          cursor?: string | null;
          error?: string;
          isDone: boolean;
          latestEnd?: number;
          latestStart: number;
          name: string;
          next?: Array<string>;
          processed: number;
          state: "inProgress" | "success" | "failed" | "canceled" | "unknown";
        }>
      >;
      clearAll: FunctionReference<
        "mutation",
        "internal",
        { before?: number },
        null
      >;
      getStatus: FunctionReference<
        "query",
        "internal",
        { limit?: number; names?: Array<string> },
        Array<{
          batchSize?: number;
          cursor?: string | null;
          error?: string;
          isDone: boolean;
          latestEnd?: number;
          latestStart: number;
          name: string;
          next?: Array<string>;
          processed: number;
          state: "inProgress" | "success" | "failed" | "canceled" | "unknown";
        }>
      >;
      migrate: FunctionReference<
        "mutation",
        "internal",
        {
          batchSize?: number;
          cursor?: string | null;
          dryRun: boolean;
          fnHandle: string;
          name: string;
          next?: Array<{ fnHandle: string; name: string }>;
          oneBatchOnly?: boolean;
        },
        {
          batchSize?: number;
          cursor?: string | null;
          error?: string;
          isDone: boolean;
          latestEnd?: number;
          latestStart: number;
          name: string;
          next?: Array<string>;
          processed: number;
          state: "inProgress" | "success" | "failed" | "canceled" | "unknown";
        }
      >;
    };
  };
};
