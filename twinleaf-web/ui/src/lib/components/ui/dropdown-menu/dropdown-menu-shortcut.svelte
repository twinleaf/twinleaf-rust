<script>
	// Shows only while hints show (prefs.svelte.js), so a menu without them fits its labels.
	import { cn } from "$lib/utils.js";
	import { prefs } from "$lib/prefs.svelte.js";
	let {
		ref = $bindable(null),
		class: className,
		children,
		...restProps
	} = $props();
</script>

{#if prefs.hints}
<span
	bind:this={ref}
	data-slot="dropdown-menu-shortcut"
	class={cn("text-muted-foreground group-focus/dropdown-menu-item:text-accent-foreground ml-auto pl-6 text-xs", className)}
	{...restProps}
>
	{@render children?.()}
</span>
{/if}
