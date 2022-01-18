import * as wf from "@temporalio/workflow";
import { SignalDefinition, QueryDefinition } from "@temporalio/common";

const noop = async () => {};
type EntityOptions = {
  maxIterations: number;
  timeToContinue: number | string
}
export class Entity<Input = any, Update = any, State extends string = any> {
  options: EntityOptions;
  setup: (input: Input) => Promise<void>;
  cleanup: (state?: State) => Promise<void>;
  updateCallback: (input?: Update) => Promise<void | State>;
  signal: SignalDefinition<[Update]>;
  state: State
  query: QueryDefinition<any>;

  constructor(
    updateCallback = noop,
    initialState = 'No initial state specified for Entity' as State,
    setup = noop,
    cleanup = noop,
    options: EntityOptions
  ) {
    this.state = initialState;
    this.updateCallback = updateCallback;
    this.setup = setup;
    this.cleanup = cleanup;
    this.signal = wf.defineSignal<[Update]>("EntitySignal");
    this.query = wf.defineQuery<[Update]>("EntityStateQuery");
    this.workflow = this.workflow.bind(this);
    this.options = {
      maxIterations: options.maxIterations || 1000,
      timeToContinue: options.timeToContinue || '1 day',
    }
  }

  async workflow(input: Input, isContinued = false) {
    try {
      const pendingUpdates = Array<Update>();
      wf.setHandler(this.signal, (updateCommand: Update) => {
        pendingUpdates.push(updateCommand);
      });
      wf.setHandler(this.query, () => this.state);

      if (!isContinued) await this.setup(input);

      for (let iteration = 1; iteration <= this.options.maxIterations; ++iteration) {
        // Automatically continue as new after a day if no updates were received
        await wf.condition(() => pendingUpdates.length > 0, this.options.timeToContinue);

        while (pendingUpdates.length) {
          const update = pendingUpdates.shift();
          await this.updateCallback(update);
        }
      }
    } catch (err) {
      if (wf.isCancellation(err)) {
        await wf.CancellationScope.nonCancellable(async () => {
          await this.cleanup(this.state);
        });
      }
      throw err;
    }
    await wf.continueAsNew<typeof this.workflow>(input, true);
  }
}
