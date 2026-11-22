<script>
	import { cn } from "$lib/utils.js";
	let {
		ref = $bindable(null),
		class: className,
		children,
		...restProps
	} = $props();
</script>

<div
	bind:this={ref}
	data-slot="alert-dialog-footer"
	class={cn(
		"flex justify-end gap-2 pt-1",
		className
	)}
	{...restProps}
>
	{@render children?.()}
</div>