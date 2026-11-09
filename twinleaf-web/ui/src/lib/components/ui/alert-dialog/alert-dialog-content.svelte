<script>
	import { AlertDialog as AlertDialogPrimitive } from "bits-ui";
	import { cn } from "$lib/utils.js";
	import { overlayContent } from "$lib/overlay.svelte.js";
	import AlertDialogOverlay from "./alert-dialog-overlay.svelte";
	import AlertDialogPortal from "./alert-dialog-portal.svelte";
	let {
		ref = $bindable(null),
		class: className,
		size = "default",
		portalProps,
		onCloseAutoFocus,
		...restProps
	} = $props();
	// What opens inside the dialog is its own, and focus goes back as it closes (overlay.svelte.js).
	const layer = overlayContent();
</script>

<AlertDialogPortal {...portalProps}>
	<AlertDialogOverlay />
	<AlertDialogPrimitive.Content
		bind:ref
		data-slot="alert-dialog-content"
		data-size={size}
		class={cn(
			" data-closed:animate-out data-closed:fade-out-0 bg-popover text-popover-foreground ring-foreground/10 gap-3 rounded-lg p-4 shadow-lg ring-1 duration-100 max-w-[min(22rem,calc(100%-2rem))] group/alert-dialog-content fixed top-1/2 left-1/2 z-50 grid w-max -translate-x-1/2 -translate-y-1/2 outline-none",
			className
		)}
		{...restProps}
		onCloseAutoFocus={(e) => (layer.closing(e), onCloseAutoFocus?.(e))}
	/>
</AlertDialogPortal>