const {src, dest, series, parallel, watch} = require('gulp');
const path = require('path');
const fs = require('fs-sync');

const {opt,
       VERSION,
       API_VERSION,
       BUILD_VERSION,
       PRODUCTION_API_HOST,
       DEV_REDIRECT_URL} = require('../build/gulp/config');
const {expandShadowTemplate} = require('../build/gulp/shadow_template');
const {makeShadowTaskCreators} = require('../build/gulp/shadow_task');
const {makeCopyTargetCreator} = require('../build/gulp/copy_target');

expandShadowTemplate("../project.clj", "shadow-cljs.template.edn", "shadow-cljs.edn");

const TARGET_CONFIG = {
  "node-test":
  {"shadow-build": "node-test",
   "shadow-conf": {}},

  "watch-node-test":
  {"shadow-verb": "watch",
   "shadow-build": "node-test-autorun",
   "shadow-conf": {}},
};

const {createShadowTask,
       createShadowReportTask} = makeShadowTaskCreators(TARGET_CONFIG);

const createCopyTargetTask = makeCopyTargetCreator(TARGET_CONFIG);

function outPath(target) {
  return path.join(__dirname, 'target', target);
}

function createCleanTask(target) {
  return function cleanTask(cb) {
    fs.remove(outPath(target));
    cb(0);
  };
}

function createTargetTasks([target, config]) {
  exports['clean-' + target] = createCleanTask(target);
  exports['shadow-' + target] = createShadowTask("release", target);
  exports['report-' + target] = createShadowReportTask(target);
}

Object.entries(TARGET_CONFIG).map(createTargetTasks);
