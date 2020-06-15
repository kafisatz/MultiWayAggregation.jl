module MultiWayAggregation

@assert VERSION>=v"1.4"

using DataFrames
#using CSV
using IterTools

export multiwayaggregation
function multiwayaggregation(df::DataFrame,v::Symbol,cs::Union{Pair, typeof(nrow), DataFrames.ColumnIndex, DataFrames.MultiColumnIndex}...)
    res=multiwayaggregation(df,vcat(v),cs...)
    return res 
end 

function multiwayaggregation(df::DataFrame,v::Vector{Symbol},cs::Union{Pair, typeof(nrow), DataFrames.ColumnIndex, DataFrames.MultiColumnIndex}...)
    res=DataFrame()

    for c in v 
        @assert !(any(ismissing,df[!,c])) #otherwise the appending will not be meaningful, as we set the values to missing for columns which are not considered in the multi way summary
    end
    
    for subsetlength=length(v):-1:0
        for subs in IterTools.subsets(v,subsetlength)
            #@show subs
            if subsetlength==0 
                agg = DataFrames.combine(df,cs...)
            else 
                agg = DataFrames.combine(DataFrames.groupby(df,subs),cs...)
            end
            nonAggregatedVars=setdiff(v,subs)
            
            k=1
            DataFrames.insertcols!(agg,k,:_TYPE_ => repeat(vcat(subsetlength),size(agg,1)))
            k+=1
            for addcol in nonAggregatedVars 
                DataFrames.insertcols!(agg,k,addcol => repeat(vcat(missing),size(agg,1)))
                k+=1
            end 
            
            DataFrames.allowmissing!(agg)
            DataFrames.append!(res,agg)            
        end
    end
    
    sort!(res,vcat(:_TYPE_,v))
    return res 
end


end # module
