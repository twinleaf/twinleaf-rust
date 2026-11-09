import { mount } from 'svelte';
import App from './App.svelte';
import { Hub } from './lib/hub.svelte.js';
import { Workspace } from './lib/workspace.svelte.js';
import { History } from './lib/history.svelte.js';
import { favorites } from './lib/favorites.svelte.js';
import { loadTabs } from './lib/tabs.js';
import './app.css';
import './plot.css';

// The page's state, made here so a test can mount the page with its own:
// the hub, the tabs, their undo and redo, and the set of PlotViews the
// frame loop draws.
const hub = new Hub();
const ws = new Workspace(loadTabs(localStorage));
const history = new History(ws, favorites);
const registry = new Set();

mount(App, { target: document.getElementById('app'), context: new Map([['page', { hub, ws, history, registry }]]) });
