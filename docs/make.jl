using Documenter, MultiWayAggregation

makedocs(
    modules = [MultiWayAggregation],
    format = Documenter.HTML(; prettyurls = get(ENV, "CI", nothing) == "true"),
    authors = "bernhard",
    sitename = "MultiWayAggregation.jl",
    pages = Any["index.md"]
    # strict = true,
    # clean = true,
    # checkdocs = :exports,
)

deploydocs(
    repo = "github.com/kafisatz/MultiWayAggregation.jl.git",
    push_preview = true
)
