<script>
	import { DropdownMenu as DropdownMenuPrimitive } from "bits-ui";
	import { cn } from "$lib/utils.js";
	import { overlayContent } from "$lib/overlay.svelte.js";
	import DropdownMenuPortal from "./dropdown-menu-portal.svelte";
	let {
		ref = $bindable(null),
		sideOffset = 4,
		// The page never scrolls, and the scroll lock blocks every press outside the
		// menu, so a press on another menu's button only closed this one. Without
		// it, the press reaches its target, and the page's one-menu rule closes this.
		preventScroll = false,
		align = "start",
		portalProps,
		class: className,
		onInteractOutside,
		onCloseAutoFocus,
		...restProps
	} = $props();
	// A press outside, or another layer opening, leaves focus where it put it (overlay.svelte.js).
	const layer = overlayContent();
</script>

<DropdownMenuPortal {...portalProps}>
	<DropdownMenuPrimitive.Content
		bind:ref
		data-slot="dropdown-menu-content"
		{sideOffset}
		{preventScroll}
		{align}
		class={cn(
			" data-closed:animate-out data-closed:fade-out-0 ring-foreground/10 bg-popover text-popover-foreground min-w-36 rounded-md p-1 shadow-lg ring-1 duration-100 z-50 max-h-(--bits-dropdown-menu-content-available-height) w-(--bits-dropdown-menu-anchor-width) origin-(--bits-dropdown-menu-content-transform-origin) overflow-x-hidden overflow-y-auto data-closed:overflow-hidden",
			className
		)}
		{...restProps}
		onInteractOutside={(e) => (layer.outside(), onInteractOutside?.(e))}
		onCloseAutoFocus={(e) => (layer.closing(e), onCloseAutoFocus?.(e))}
	/>
</DropdownMenuPortal>