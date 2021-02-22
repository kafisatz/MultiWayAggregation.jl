module MultiWayAggregation

@assert VERSION>=v"1.4"

using DataFrames
##using CSV
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

#multiwayaggregationkw 
#same as multiwayaggregation but instead of 'missing' a keyword (e.g. "ALL") is used to denote subtotal summaries
export multiwayaggregationkw
function multiwayaggregationkw(df::DataFrame,v::Symbol,subtotalkw,cs::Union{Pair, typeof(nrow), DataFrames.ColumnIndex, DataFrames.MultiColumnIndex}...)
    res=multiwayaggregationkw(df,vcat(v),subtotalkw,cs...)
    return res 
end 

function multiwayaggregationkw(df::DataFrame,v::Vector{Symbol},subtotalkw,cs::Union{Pair, typeof(nrow), DataFrames.ColumnIndex, DataFrames.MultiColumnIndex}...)
    res=DataFrame()

    for c in v 
        @assert eltype(df[!,c]) == typeof(subtotalkw)
        @assert !(any(isequal(subtotalkw),df[!,c])) #otherwise the appending will not be meaningful, as we set the values to missing for columns which are not considered in the multi way summary
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
                DataFrames.insertcols!(agg,k,addcol => repeat(vcat(subtotalkw),size(agg,1)))
                k+=1
            end 
            
            DataFrames.allowmissing!(agg)
            DataFrames.append!(res,agg)            
        end
    end
    
    sort!(res,vcat(:_TYPE_,v))
    return res 
end


export addkey! 

#adds comma separated row identifier to each row 
function addkey!(df,v::Vector{Symbol};keyname=:key)
    @assert issubset(v,propertynames(df))
    @assert !in(keyname,propertynames(df))
    @assert !in(keyname,v)

    df[!,keyname]=repeat(vcat(""),size(df,1))
    for i=1:size(df,1)
        df[i,keyname]  = join(df[i,v],",")
    end 
    
    have=vcat(keyname)
    donthave=setdiff(propertynames(df),have)
    select!(df,vcat(have,donthave))

    return nothing 
end


end # module
