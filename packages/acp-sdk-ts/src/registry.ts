/** Component registry: id -> ComponentDef, with descriptor projection. */
import type { ComponentDef } from "./component.js";
import { toDescriptor } from "./component.js";
import type { ComponentDescriptor } from "./types.js";
import { isValidComponentId } from "./codec.js";

/** Variance-loose form used by the registry (handlers keep their precise types at definition sites). */
export type AnyComponentDef = ComponentDef<any, any>;

export class Registry {
  #components = new Map<string, AnyComponentDef>();

  register(def: AnyComponentDef): void {
    if (!isValidComponentId(def.id)) {
      throw new Error(`invalid component id: ${def.id} (spec §7.1)`);
    }
    if (typeof def.handle !== "function") {
      throw new Error(`component ${def.id}: handle must be a function`);
    }
    if (this.#components.has(def.id)) {
      throw new Error(`component already registered: ${def.id}`);
    }
    this.#components.set(def.id, def);
  }

  get(id: string): ComponentDef | undefined {
    return this.#components.get(id);
  }

  has(id: string): boolean {
    return this.#components.has(id);
  }

  list(): AnyComponentDef[] {
    return [...this.#components.values()];
  }

  descriptors(): ComponentDescriptor[] {
    return this.list().map(toDescriptor);
  }

  get size(): number {
    return this.#components.size;
  }
}
