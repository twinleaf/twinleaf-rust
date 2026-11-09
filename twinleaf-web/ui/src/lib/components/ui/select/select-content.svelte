<script>
	import { Select as SelectPrimitive } from "bits-ui";
	import { cn } from "$lib/utils.js";
	import { overlayContent } from "$lib/overlay.svelte.js";
	import SelectPortal from "./select-portal.svelte";
	import SelectScrollDownButton from "./select-scroll-down-button.svelte";
	import SelectScrollUpButton from "./select-scroll-up-button.svelte";
	let {
		ref = $bindable(null),
		class: className,
		sideOffset = 4,
		// A list opens under its trigger, from the trigger's left edge.
		align = "start",
		portalProps,
		children,
		// The page never scrolls, and the scroll lock blocks every press outside the
		// menu, so a press on another menu's button only closed this one. Without
		// it, the press reaches its target, and the page's one-menu rule closes this.
		preventScroll = false,
		onInteractOutside,
		onCloseAutoFocus,
		...restProps
	} = $props();
	// A press outside, or another layer opening, leaves focus where it put it (overlay.svelte.js).
	const layer = overlayContent();
</script>

<SelectPortal {...portalProps}>
	<SelectPrimitive.Content
		bind:ref
		{sideOffset}
		{align}
		{preventScroll}
		data-slot="select-content"
		class={cn(
			"bg-popover text-popover-foreground data-closed:animate-out data-closed:fade-out-0 ring-foreground/10 min-w-36 rounded-lg shadow-md ring-1 duration-100 relative z-50 max-h-(--bits-select-content-available-height) origin-(--bits-select-content-transform-origin) overflow-x-hidden overflow-y-auto",
			className
		)}
		{...restProps}
		onInteractOutside={(e) => (layer.outside(), onInteractOutside?.(e))}
		onCloseAutoFocus={(e) => (layer.closing(e), onCloseAutoFocus?.(e))}
	>
		<SelectScrollUpButton />
		<SelectPrimitive.Viewport
			class={cn(
				"h-(--bits-select-anchor-height) w-full min-w-(--bits-select-anchor-width) scroll-my-1"
			)}
		>
			{@render children?.()}
		</SelectPrimitive.Viewport>
		<SelectScrollDownButton />
	</SelectPrimitive.Content>
</SelectPortal>