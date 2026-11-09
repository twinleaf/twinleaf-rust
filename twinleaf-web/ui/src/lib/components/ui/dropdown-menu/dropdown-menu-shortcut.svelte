<script>
	// A hint: it shows only while the hints show (prefs.svelte.js), so a menu
	// without them fits its labels, and its row's value takes its place.
	import { cn } from "$lib/utils.js";
	let {
		ref = $bindable(null),
		class: className,
		children,
		...restProps
	} = $props();
</script>

<span
	bind:this={ref}
	data-slot="dropdown-menu-shortcut"
	data-hint
	class={cn("text-muted-foreground group-focus/dropdown-menu-item:text-accent-foreground ml-auto pl-6 text-xs", className)}
	{...restProps}
>
	{@render children?.()}
</span>
