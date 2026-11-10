<script>
	import { Popover as PopoverPrimitive } from "bits-ui";
	import { cn } from "$lib/utils.js";
	import { overlayContent } from "$lib/overlay.svelte.js";
	import PopoverPortal from "./popover-portal.svelte";
	let {
		ref = $bindable(null),
		class: className,
		sideOffset = 4,
		align = "center",
		portalProps,
		onInteractOutside,
		onCloseAutoFocus,
		...restProps
	} = $props();
	// A press outside, or another layer opening, leaves focus where it put it (overlay.svelte.js).
	const layer = overlayContent();
</script>

<PopoverPortal {...portalProps}>
	<PopoverPrimitive.Content
		bind:ref
		data-slot="popover-content"
		{sideOffset}
		{align}
		class={cn(
			"bg-popover text-popover-foreground data-closed:animate-out data-closed:fade-out-0 ring-foreground/10 flex flex-col gap-2.5 rounded-lg p-2.5 text-sm shadow-md ring-1 duration-100 z-50 w-72 max-h-(--bits-popover-content-available-height) overflow-y-auto origin-(--bits-popover-content-transform-origin) outline-hidden",
			className
		)}
		{...restProps}
		onInteractOutside={(e) => (layer.outside(), onInteractOutside?.(e))}
		onCloseAutoFocus={(e) => (layer.closing(e), onCloseAutoFocus?.(e))}
	/>
</PopoverPortal>