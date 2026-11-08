<script>
	import { ContextMenu as ContextMenuPrimitive } from "bits-ui";
	import { cn } from "$lib/utils.js";

	let {
		ref = $bindable(null),
		class: className,
		inset,
		...restProps
	} = $props();
</script>

<ContextMenuPrimitive.GroupHeading
	bind:ref
	data-slot="context-menu-group-heading"
	data-inset={inset}
	class={cn("text-muted-foreground pt-1.5 pb-0.5 pr-2 pl-6 text-[11px] font-medium data-inset:pl-7", className)}
	{...restProps}
/>