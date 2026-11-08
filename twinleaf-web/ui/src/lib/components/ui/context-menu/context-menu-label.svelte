<script>
	import { cn } from "$lib/utils.js";
	let {
		ref = $bindable(null),
		class: className,
		inset,
		children,
		...restProps
	} = $props();
</script>

<div
	bind:this={ref}
	data-slot="context-menu-label"
	data-inset={inset}
	class={cn("text-muted-foreground pt-1.5 pb-0.5 pr-2 pl-6 text-[11px] font-medium data-inset:pl-7", className)}
	{...restProps}
>
	{@render children?.()}
</div>