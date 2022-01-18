# temporal-time-utils

This is a library with some reusable functions for [Temporal.io TypeScript SDK](https://docs.temporal.io/docs/typescript/introduction):

- `sleepUntil`: sleep to a specific date, instead of by num of milliseconds
- `UpdatableTimer`: sleep to a specific date, updatable and queryable
- `ScheduleWorkflow`: schedule other workflows by extended cron syntax, with support for jitter, timezone, max invocations, manual triggers, pausing/unpausing, querying future executions, and more.

This serves as both a utility library and a demonstration of how you can publish reusable Temporal libraries (both for Workflows and Activities).

```ts
npm i temporal-time-utils
```

Repo: https://github.com/sw-yx/temporal-time-utils

## `sleepUntil`

This is a simple drop in replacement for Temporal's `workflow.sleep()` API for when you need to sleep to a specific date rather than a fixed number of milliseconds.

```ts
import { sleepUntil } from "temporal-time-utils";

// inside workflow function
sleepUntil("30 Sep " + (new Date().getFullYear() + 1)); // wake up when September ends
sleepUntil("5 Nov 2022 00:12:34 GMT"); // wake up at specific time and timezone

// optional 2nd arg
sleepUntil("5 Nov 2022 00:12:34 GMT", specificDateTime); // take control over the "start" time in case you need to
```

This is a very simple function - under the hood it just uses `date-fns/differenceInMilliseconds` to calculate time difference.

This is discussed in the docs https://docs.temporal.io/docs/typescript/workflows#sleep

## `UpdatableTimer`

`sleep` only lets you set a fixed time upfront.
`UpdatableTimer` is a special class that lets you update that while sleeping.
You can consider it the next step up from `sleepUntil`.

After you instantiate it with an initial datetime to wake up at, it exposes only two APIs: `then()` for you to `await`, and `.deadline` that you can set and get.

```ts
import { UpdatableTimer } from "temporal-time-utils";

// example usage inside workflow function
export async function countdownWorkflow(initialDeadline: Date): Promise<void> {
  const timer = new UpdatableTimer(initialDeadline);
  wf.setHandler(
    setDeadlineSignal,
    (deadline) => void (timer.deadline = deadline)
  ); // send in new deadlines via Signal
  wf.setHandler(timeLeftQuery, () => timer.deadline - Date.now()); // get time left via Query
  await timer; // if you send in a signal with a new time, this timer will resolve earlier!
  console.log("countdown done!");
}
```

This is discussed in the docs https://docs.temporal.io/docs/typescript/workflows#async-design-patterns.

## `ScheduleWorkflow`

A Workflow that schedules other Workflows.

This is a premade Workflow that you can register in a Worker and call from a client, to invoke other Workflows on a schedule. You can consider this the next step up from "Cron Workflows".

See example usage inside of `/apps/fixture`:

- https://github.com/sw-yx/temporal-time-utils/blob/main/apps/fixture/src/client.ts invoke the `ScheduleWorkflow` and pass in an `example` Workflow to call on a schedule
- https://github.com/sw-yx/temporal-time-utils/blob/main/apps/fixture/src/workflows.ts#L5 necessary export for Worker to pick it up

```ts
import { ScheduleWorkflow } from "temporal-time-utils";

// inside client file
async function run() {
  const client = new WorkflowClient();
  const handle = await client.start(ScheduleWorkflow, {
    args: [
      "exampleWorkflow", // workflow to be executed on a schedule. must be string name.
      {
        args: ["Example arg payload"], // static for now, but possible to modify to make dynamic in future - ask swyx
        // // regular workflow options apply here, with two additions (defaults shown):
        // cancellationType: ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
        // parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_TERMINATE
      },
      {
        // args
        cronParser: {
          expression: "* * * * *", // every minute
          // options: https://www.npmjs.com/package/cron-parser#user-content-options
        },
        // maxInvocations?: number;
        // jitterMs?: number;
      },
    ],
    taskQueue: "tutorial",
    workflowId: "my-schedule-id",
  });
}
```

This uses https://www.npmjs.com/package/cron-parser under the hood, thereby getting support for things like timezones and "last of the week" extensions to the cron syntax:

```ts
// client.ts
const handle = await client.start(ScheduleWorkflow, {
  args: [
    "exampleWorkflow", // as above
    {}, // if no args needed
    {
      // scheduleOptions
      cronParser: {
        expression: "0 0 * * * 1,3L", // run every Monday as well as the last Wednesday of the month
        options: {
          currentDate: "2016-03-27 00:00:01",
          endDate: new Date("Wed, 26 Dec 2012 14:40:00 UTC"),
          tz: "Europe/Athens",
        },
      },
      maxInvocations: 500,
      jitterMs: 1000,
    },
  ],
  taskQueue: "scheduler",
  workflowId: "schedule-for-" + userId,
});
```

The Workflow exposes a number of useful queries and signals:

```ts
import {
  numInvocationsQuery,
  futureScheduleQuery,
  manualTriggerSignal,
  ScheduleWorkflowState,
  stateSignal,
  stateQuery,
  // ...
} from "temporal-time-utils";

await handle.query(numInvocationsQuery); // get how many times exampleWorkflow has been invoked by ScheduleWorkflow
await handle.query(futureScheduleQuery, 3); // get the next 3 times it is set to be invoked. defaults to 5
await handle.signal(manualTriggerSignal); // manually trigger workflow
await handle.signal(stateSignal, "PAUSED" as ScheduleWorkflowState); // pause workflow
await handle.signal(stateSignal, "RUNNING" as ScheduleWorkflowState); // resume workflow
await handle.cancel(); // stop schedule workflow completely
await handle.query(stateQuery); // get wf state (running, paused, or stopped)
```

## `Entity`

This special class packages an indefinitely long lived Workflow and the Signal and Query that go with updating it. It correctly handles the pending Signals and `continueAsNew`, and calls `continueAsNew` at least once a day as recommended by Temporal.

```ts
import { Entity } from "temporal-time-utils";

interface Input { /* define your workflow input type here */ }
interface Update { /* define your workflow update type here */ }
const entity = new Entity<Input, Update>({
  activity: 'MyActivityName'
  activityOptions: {
    startToCloseTimeout: '1 minute',
  }
})
const handle = await client.start(entity.workflow, {
  args: [{
    inputValue: 'initial'
  }],
  taskQueue: "scheduler",
  workflowId: "schedule-for-" + userId,
});

// during signaling updates
await client.Signal(entity.Signal, { increment: 1 })
console.log(await client.Query(entity.Query))
```
