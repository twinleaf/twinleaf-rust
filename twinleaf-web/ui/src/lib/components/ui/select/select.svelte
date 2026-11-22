<script>
	// One popup layer open at a time (overlay.svelte.js).
	import { Select as SelectPrimitive } from "bits-ui";
	import { overlayRoot } from "$lib/overlay.svelte.js";

	let {
		open = $bindable(false),
		value = $bindable(),
		...restProps
	} = $props();
	const shown = overlayRoot(() => (open = false));
	$effect(() => shown(open));
</script>

<SelectPrimitive.Root bind:open bind:value={value} {...restProps} />
