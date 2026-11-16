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
	data-slot="alert-dialog-header"
	class={cn("grid gap-1 text-left", className)}
	{...restProps}
>
	{@render children?.()}
</div>