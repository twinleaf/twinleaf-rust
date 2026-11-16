<script>
	// One popup layer open at a time (overlay.svelte.js).
	import { ContextMenu as ContextMenuPrimitive } from "bits-ui";
	import { overlayRoot } from "$lib/overlay.svelte.js";

	let { open = $bindable(false), ...restProps } = $props();
	const shown = overlayRoot(() => (open = false));
	$effect(() => shown(open));
</script>

<ContextMenuPrimitive.Root bind:open {...restProps} />
