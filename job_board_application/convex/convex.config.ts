// convex/convex.config.ts
import { defineApp } from "convex/server";
import crons from "@convex-dev/crons/convex.config.js";
import migrations from "@convex-dev/migrations/convex.config.js";
import aggregate from "@convex-dev/aggregate/convex.config.js";

const app = defineApp();
app.use(crons);
app.use(migrations);
app.use(aggregate);

export default app;
