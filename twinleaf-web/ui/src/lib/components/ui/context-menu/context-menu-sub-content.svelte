<script>
	import { ContextMenu as ContextMenuPrimitive } from "bits-ui";
	import { cn } from "$lib/utils.js";
	import { runShownKey } from "$lib/menus.js";
	import ContextMenuPortal from "./context-menu-portal.svelte";
	let {
		ref = $bindable(null),
		class: className,
		align = "start",
		alignOffset = -3,
		portalProps,
		...restProps
	} = $props();
</script>

<ContextMenuPortal {...portalProps}>
	<ContextMenuPrimitive.SubContent
		bind:ref
		data-slot="context-menu-sub-content"
		{align}
		{alignOffset}
		class={cn(
			" data-closed:animate-out data-closed:fade-out-0 ring-foreground/10 bg-popover text-popover-foreground min-w-36 rounded-md p-1 shadow-lg ring-1 duration-100 z-50 origin-(--bits-context-menu-content-transform-origin) overflow-hidden",
			className
		)}
		{...restProps}
		onkeydowncapture={runShownKey}
	/>
</ContextMenuPortal>