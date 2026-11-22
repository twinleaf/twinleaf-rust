import { mount } from 'svelte';
import App from './App.svelte';
import { Hub } from './lib/hub.svelte.js';
import { Workspace } from './lib/workspace.svelte.js';
import { History } from './lib/history.svelte.js';
import { Views } from './lib/views.js';
import { PlotHost } from './lib/plothost.js';
import { colors } from './lib/colors.svelte.js';
import { favorites } from './lib/favorites.svelte.js';
import { loadTabs } from './lib/tabs.js';
import './app.css';
import './plot.css';

// The page's state, made here so a test can mount the page with its own:
// the hub, the tabs, their undo and redo, the set of PlotViews the frame
// loop draws, each plot's view, and the plots' host, through which each
// plot reaches the rest (plothost.js).
const hub = new Hub();
const ws = new Workspace(loadTabs(localStorage));
const history = new History(ws, favorites);
const registry = new Set();
const views = new Views(hub);
const plots = new PlotHost({ hub, ws, history, views, registry, colors });

mount(App, { target: document.getElementById('app'), context: new Map([['page', { hub, ws, history, views, plots }]]) });
