<script>
	// No tooltip shows over a popup layer it is not inside (overlay.svelte.js).
	import { Tooltip as TooltipPrimitive } from "bits-ui";
	import { tooltipHidden } from "$lib/overlay.svelte.js";

	let { open = $bindable(false), disabled = false, ...restProps } = $props();
	const hidden = tooltipHidden();
	$effect(() => {
		if (open && hidden()) open = false;
	});
</script>

<TooltipPrimitive.Root bind:open disabled={disabled || hidden()} {...restProps} />
