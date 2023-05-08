#cd("H:\\Code\\Julia\\MultiWayAggregation\\")
using MultiWayAggregation
using Test

import Downloads
using DataFrames
using CSV

@testset "Main Tests" begin

    
    local df 
    try 
        df=CSV.read(Downloads.download("https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/639388c2cbc2120a14dcf466e85730eb8be498bb/iris.csv"),DataFrame)
    catch 
        #alteranative source
        df = CSV.read(Downloads.download("https://vincentarelbundock.github.io/Rdatasets/csv/datasets/iris.csv"),DataFrame)
        select!(df,Not(:Column1))
        rename!(df,["sepal_length", "sepal_width", "petal_length", "petal_width", "species"])
    end

    v=[:species,:petal_width]
    #testing different signatures
    multiwayaggregation(df,v[1],:sepal_length=>sum)
    multiwayaggregation(df,string(v[1]),:sepal_length=>sum)
    multiwayaggregation(df,string.(v),:sepal_length=>sum)

    aggMW=multiwayaggregation(df,v,:sepal_length=>sum)

    #"manual approach"
    aggReference=combine(DataFrames.groupby(df,[:species,:petal_width]),:sepal_length=>sum)
    agg2=combine(DataFrames.groupby(df,[:species]),:sepal_length=>sum)
    agg3=combine(DataFrames.groupby(df,[:petal_width]),:sepal_length=>sum)
    agg4=DataFrames.combine(df,:sepal_length=>sum)

    append!(aggReference,agg2,cols=:union)
    append!(aggReference,agg3,cols=:union)
    append!(aggReference,agg4,cols=:union)

    svec=[:sepal_length_sum,:petal_width,:species] 
    sort!(aggReference,svec)
    sort!(aggMW,svec)
    aggMW2=select(aggMW,Not(:_TYPE_))

    for i=1:size(aggMW2,1)
        for j=1:size(aggMW2,2)
            #@show i,j
            if ismissing(aggMW2[i,j])
                @test ismissing(aggReference[i,j])
            else 
                @test aggMW2[i,j]==aggReference[i,j]
            end
        end 
    end 

    #code coverage
    aggMW3=multiwayaggregation(df,v[1],:sepal_length=>sum)
    addkey!(aggMW2,v)

end