<script>
	import { ContextMenu as ContextMenuPrimitive } from "bits-ui";
	import { cn } from "$lib/utils.js";
	import { overlayContent } from "$lib/overlay.svelte.js";
	import ContextMenuPortal from "./context-menu-portal.svelte";
	let {
		ref = $bindable(null),
		portalProps,
		// The page never scrolls, and the scroll lock blocks every press outside the
		// menu, so a press on another menu's button only closed this one. Without
		// it, the press reaches its target, and the page's one-menu rule closes this.
		preventScroll = false,
		class: className,
		onInteractOutside,
		onCloseAutoFocus,
		...restProps
	} = $props();
	// A press outside, or another layer opening, leaves focus where it put it (overlay.svelte.js).
	const layer = overlayContent();
</script>

<ContextMenuPortal {...portalProps}>
	<ContextMenuPrimitive.Content
		bind:ref
		data-slot="context-menu-content"
		{preventScroll}
		class={cn(
			" data-closed:animate-out data-closed:fade-out-0 ring-foreground/10 bg-popover text-popover-foreground min-w-36 rounded-md p-1 shadow-lg ring-1 duration-100 z-50 max-h-(--bits-context-menu-content-available-height) origin-(--bits-context-menu-content-transform-origin) overflow-x-hidden overflow-y-auto",
			className
		)}
		{...restProps}
		{@attach (el) => layer.element(el)}
		onInteractOutside={(e) => (layer.outside(), onInteractOutside?.(e))}
		onCloseAutoFocus={(e) => (layer.closing(e), onCloseAutoFocus?.(e))}
	/>
</ContextMenuPortal>