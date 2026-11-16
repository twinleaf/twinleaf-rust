<script>
	// One popup layer open at a time (overlay.svelte.js).
	import { DropdownMenu as DropdownMenuPrimitive } from "bits-ui";
	import { overlayRoot } from "$lib/overlay.svelte.js";

	let { open = $bindable(false), ...restProps } = $props();
	const shown = overlayRoot(() => (open = false));
	$effect(() => shown(open));
</script>

<DropdownMenuPrimitive.Root bind:open {...restProps} />
