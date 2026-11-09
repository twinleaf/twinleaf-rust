<script>
	// One popup layer open at a time (overlay.svelte.js).
	import { AlertDialog as AlertDialogPrimitive } from "bits-ui";
	import { overlayRoot } from "$lib/overlay.svelte.js";

	let { open = $bindable(false), ...restProps } = $props();
	const shown = overlayRoot(() => (open = false));
	$effect(() => shown(open));
</script>

<AlertDialogPrimitive.Root bind:open {...restProps} />
