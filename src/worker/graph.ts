import type { Definition, Json, Step } from '../workflows/definition';
export type StepStatus =
  | 'pending'
  | 'ready'
  | 'running'
  | 'waiting'
  | 'succeeded'
  | 'failed'
  | 'skipped';
export interface StepState {
  status: StepStatus;
  output?: Json;
}
export function readyTransitions(
  definition: Definition,
  states: Record<string, StepState>,
): { ready: string[]; skipped: string[] } {
  const byId = new Map(definition.steps.map((step) => [step.id, step]));
  const virtuallySkipped = new Set<string>();
  const classifyDependencies = (step: Step) => {
    let unresolved = false;
    let failed = false;
    let active = 0;
    for (const dependency of step.needs ?? []) {
      const status = virtuallySkipped.has(dependency.step)
        ? 'skipped'
        : (states[dependency.step]?.status ?? 'pending');
      if (
        status === 'pending' ||
        status === 'ready' ||
        status === 'running' ||
        status === 'waiting'
      ) {
        unresolved = true;
      } else if (status === 'failed') {
        failed = true;
      } else if (status === 'succeeded') {
        const predecessor = byId.get(dependency.step)!;
        if (
          predecessor.type !== 'condition' ||
          states[dependency.step]?.output === dependency.when
        ) {
          active += 1;
        }
      }
    }
    return { active, failed, unresolved };
  };
  let changed = true;
  while (changed) {
    changed = false;
    for (const step of definition.steps) {
      if (
        (states[step.id]?.status ?? 'pending') !== 'pending' ||
        virtuallySkipped.has(step.id) ||
        !step.needs?.length
      )
        continue;
      const { active, failed, unresolved } = classifyDependencies(step);
      if (!unresolved && !failed && active === 0) {
        virtuallySkipped.add(step.id);
        changed = true;
      }
    }
  }
  const ready: string[] = [];
  const skipped: string[] = [];
  for (const step of definition.steps) {
    if (virtuallySkipped.has(step.id)) {
      skipped.push(step.id);
      continue;
    }
    if ((states[step.id]?.status ?? 'pending') !== 'pending') continue;
    if (!step.needs?.length) {
      ready.push(step.id);
      continue;
    }
    const { active, failed, unresolved } = classifyDependencies(step);
    if (!unresolved && !failed && active > 0) ready.push(step.id);
  }
  return { ready, skipped };
}
