// XGBoost inference over the trees exported by 04_web_export.R. No DOM or
// fetch calls, so the parity tests can run it in Node.
import { invLogit } from "./stats";
import type { ModelIndex, XgbModel } from "./types";

export function decodeModel(index: ModelIndex, buf: ArrayBuffer): XgbModel {
  const { n_trees, n_nodes } = index;
  let offset = 0;
  const i32 = (len: number) => {
    const view = new Int32Array(buf, offset, len);
    offset += 4 * len;
    return view;
  };
  const treeOffset = i32(n_trees + 1);
  const left = i32(n_nodes);
  const right = i32(n_nodes);
  const value = new Float32Array(buf, offset, n_nodes);
  offset += 4 * n_nodes;
  const feature = new Uint8Array(buf, offset, n_nodes);
  offset += n_nodes;
  const defaultLeft = new Uint8Array(buf, offset, n_nodes);
  return {
    nTrees: n_trees, treeOffset, left, right, value, feature, defaultLeft,
    baseMargin: index.base_margin, featureNames: index.feature_names,
  };
}

// predict(model, xgb.DMatrix(X)) for one row of a binary:logistic model.
// xgboost stores features as float32 and sends a row left when x < threshold.
export function predictProba(model: XgbModel, features: Record<string, number>): number {
  const x = new Float32Array(model.featureNames.length);
  model.featureNames.forEach((name, i) => {
    if (!(name in features)) throw new Error(`Missing model feature: ${name}`);
    x[i] = features[name];
  });
  const { treeOffset, left, right, value, feature, defaultLeft } = model;
  let margin = model.baseMargin;
  for (let t = 0; t < model.nTrees; t++) {
    const off = treeOffset[t];
    let node = 0;
    while (left[off + node] !== -1) {
      const k = off + node;
      const v = x[feature[k]];
      const goLeft = Number.isNaN(v) ? defaultLeft[k] === 1 : v < value[k];
      node = goLeft ? left[k] : right[k];
    }
    margin += value[off + node];
  }
  return invLogit(margin);
}
