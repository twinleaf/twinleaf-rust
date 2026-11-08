# Focus and keys on the web page

How the keyboard moves through the page, and where a click leaves it. Every
interactive part of the page sits in one tree. One cursor in that tree says
where the keyboard is, one handler moves it (`focus.js`), and one ring shows
it (`FocusRing.svelte`).

## The tree

```
page
├─ toolbar        Commands · Live · Window · Clear · Layout · File · Settings
├─ left dock      hide button · panel headers
│  └─ panel …     its contents (Devices: rows, Find · Signals: search, list
│                 · Properties: table, edit bar · Actions: buttons)
├─ tab strip      tabs (one stop; ← → move between them) · +
├─ plots
│  └─ plot …      kind · layout · signals · legend · pause · split right ·
│                 split down · maximize · close
├─ right dock     (as the left)
└─ status bar     History · Keys
```

Splitters are not in the tree. The command palette resizes the docks.

## The rules

1. **The cursor is one node.** A click puts it on the nearest node under the
   pointer: a button, a plot, a panel header, or a region's empty space.
   Focus arriving any other way does the same.
2. **Tab and Shift+Tab move between siblings** at the cursor's level, wrapping
   at the ends. Clicking a plot's split button and pressing Tab reaches the
   next header control. Clicking a panel header and pressing Tab reaches the
   next panel header.
3. **← → ↑ ↓ do the same**, except in a node that uses them itself: a plot
   pans and zooms, a list or table moves between its rows, the tabs move
   between tabs, a field moves its caret.
4. **Enter goes one level down**, to the child the node last had, or its
   first: a plot enters its signals, a panel header its contents, which it
   unfolds first. **Esc goes one level up.** **F6 and Shift+F6 move between
   the regions**, each to where it was last.
5. **Every other key goes to the highlighted plot**, wherever the cursor is,
   unless what has focus keeps it (keys.js). F on a plot's maximize button
   maximizes that plot. ? and Ctrl K reach the page.
6. **What is open keeps its keys first.** A menu, dropdown or dialog keeps
   every key. A field keeps its typing. Esc closes or clears what it can
   first (a tooltip, a search, a plot's maximize, selection or cursors),
   then goes up.
7. **The highlighted plot is the cursor's plot**: the plot the cursor is on or
   inside. Selection (several plots, by Shift+click or the box) stays apart
   from the cursor.

A panel header is a button: a click or Space folds it, a drag moves it.

## The focus ring

One ring, drawn by the page in a layer above everything, around the
element with focus, so nothing clips it and nothing draws its own. It shows
where the browser shows focus (`:focus-visible`): after a key moved it, and
in a field however it came. Inside a menu, a list or a table, the
highlighted item shows instead.

## How a part joins the tree

By attributes, which `focus.js` reads from the page as it is when a key is
pressed:

| Attribute | Means |
|---|---|
| `data-focus="kind"` | a node: a region, a panel header, a plot |
| `data-focus-own` | a node that moves among its own items; nothing inside is a node |
| `data-focus-arrows` | a node that uses the arrows itself |
| `data-focus-key` / `data-focus-group` | a node whose children sit elsewhere, as a panel header's in its body |
| `data-focus-skip` | left out, with everything inside it |

Every control inside a node (a button, a field, a tab) is a node too, a
child of the nearest node around it. A component that takes a key for
itself calls `preventDefault`, and the tree leaves that key alone.
