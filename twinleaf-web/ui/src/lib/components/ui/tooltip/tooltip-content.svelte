<script module>
	/** How near, in px, a tooltip comes to the window's edge: it flips or shifts to stay this far inside, and wraps to fit between the two edges. */
	export const TIP_EDGE = 4;
</script>

<script>
	import { Tooltip as TooltipPrimitive } from "bits-ui";
	import { cn } from "$lib/utils.js";
	import TooltipPortal from "./tooltip-portal.svelte";
	let {
		ref = $bindable(null),
		class: className,
		sideOffset = 0,
		collisionPadding = TIP_EDGE,
		side = "top",
		children,
		arrowClasses,
		portalProps,
		...restProps
	} = $props();
</script>

<TooltipPortal {...portalProps}>
	<TooltipPrimitive.Content
		bind:ref
		data-slot="tooltip-content"
		{sideOffset}
		{collisionPadding}
		{side}
		style="max-width: min(20rem, 100vw - {2 * TIP_EDGE}px); overflow-wrap: anywhere"
		class={cn(
			" data-closed:animate-out data-closed:fade-out-0 inline-flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs has-data-[slot=kbd]:pr-1.5 **:data-[slot=kbd]:relative **:data-[slot=kbd]:isolate **:data-[slot=kbd]:z-50 **:data-[slot=kbd]:rounded-sm z-50 w-fit origin-(--bits-tooltip-content-transform-origin) bg-foreground text-background",
			className
		)}
		{...restProps}
	>
		{@render children?.()}
		<TooltipPrimitive.Arrow>
			{#snippet child({ props })}
				<div
					class={cn(
						"size-2.5 translate-y-[calc(-50%_-_2px)] rotate-45 rounded-[2px] z-50 bg-foreground fill-foreground data-[side=bottom]:-translate-x-1/2 data-[side=bottom]:-translate-y-[calc(-50%_+_1px)] data-[side=left]:-translate-y-[calc(50%_-_3px)] data-[side=right]:translate-x-[calc(50%_+_2px)] data-[side=right]:translate-y-1/2 data-[side=top]:translate-x-1/2 data-[side=top]:translate-y-[calc(-50%_+_2px)]",
						arrowClasses
					)}
					{...props}
				></div>
			{/snippet}
		</TooltipPrimitive.Arrow>
	</TooltipPrimitive.Content>
</TooltipPortal>