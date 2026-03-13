[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "wind_power_data_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "production_id", DestinationColumnName = "production_id"], [SourceColumnName = "date", DestinationColumnName = "date"], [SourceColumnName = "time", DestinationColumnName = "time"], [SourceColumnName = "turbine_name", DestinationColumnName = "turbine_name"], [SourceColumnName = "capacity", DestinationColumnName = "capacity"], [SourceColumnName = "location_name", DestinationColumnName = "location_name"], [SourceColumnName = "latitude", DestinationColumnName = "latitude"], [SourceColumnName = "longitude", DestinationColumnName = "longitude"], [SourceColumnName = "region", DestinationColumnName = "region"], [SourceColumnName = "status", DestinationColumnName = "status"], [SourceColumnName = "responsible_department", DestinationColumnName = "responsible_department"], [SourceColumnName = "wind_speed", DestinationColumnName = "wind_speed"], [SourceColumnName = "wind_direction", DestinationColumnName = "wind_direction"], [SourceColumnName = "energy_produced", DestinationColumnName = "energy_produced"], [SourceColumnName = "Day", DestinationColumnName = "Day"], [SourceColumnName = "Month", DestinationColumnName = "Month"], [SourceColumnName = "Quarter", DestinationColumnName = "Quarter"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "hour_of_day", DestinationColumnName = "hour_of_day"], [SourceColumnName = "minute_of_hour", DestinationColumnName = "minute_of_hour"], [SourceColumnName = "second_of_hour", DestinationColumnName = "second_of_hour"], [SourceColumnName = "Time_Period", DestinationColumnName = "Time_Period"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared wind_power_data = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d1aea760-fe6b-4e82-8724-88a548118c60"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2f3f9258-0a08-43ff-917b-e450c2de4e2c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "wind_power_data", ItemKind = "Table"]}[Data],
  #"Rounded off" = Table.TransformColumns(#"Navigation 2", {{"energy_produced", each Number.Round(_, 2), type number}, {"wind_speed", each Number.Round(_, 2), type number}}),
  #"Inserted day" = Table.AddColumn(#"Rounded off", "Day", each Date.Day([date]), type nullable number),
  #"Inserted month" = Table.AddColumn(#"Inserted day", "Month", each Date.Month([date]), type nullable number),
  #"Inserted quarter" = Table.AddColumn(#"Inserted month", "Quarter", each Date.QuarterOfYear([date]), type nullable number),
  #"Inserted year" = Table.AddColumn(#"Inserted quarter", "Year", each Date.Year([date]), type nullable number),
  #"Replaced value" = Table.ReplaceValue(#"Inserted year", "-", ":", Replacer.ReplaceText, {"time"}),
  #"Duplicated column" = Table.DuplicateColumn(#"Replaced value", "time", "time - Copy"),
  #"Split column by delimiter" = Table.SplitColumn(#"Duplicated column", "time - Copy", Splitter.SplitTextByDelimiter(":"), {"time - Copy.1", "time - Copy.2", "time - Copy.3"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Split column by delimiter", {{"time - Copy.1", Int64.Type}, {"time - Copy.2", Int64.Type}, {"time - Copy.3", Int64.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"time - Copy.1", "hour_of_day"}, {"time - Copy.2", "minute_of_hour"}, {"time - Copy.3", "second_of_hour"}}),
  #"Added custom" = Table.AddColumn(#"Renamed columns", "Time_Period", each if [hour_of_day] >=5 and [hour_of_day]<12 then "Morning"
else if [hour_of_day] >= 12 and [hour_of_day]<17 then "Afternoon"
else if [hour_of_day] >= 17 and [hour_of_day]<21 then "Evening"
else "Night"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom", {{"Day", Int64.Type}, {"Month", Int64.Type}, {"Quarter", Int64.Type}, {"Year", Int64.Type}, {"Time_Period", type text}})
in
  #"Changed column type 1";
shared wind_power_data_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d1aea760-fe6b-4e82-8724-88a548118c60"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "011b86a2-ce71-4c17-888f-3711d32205c1"]}[Data],
  TableNavigation = Navigation_2{[Id = "wind_power", ItemKind = "Table"]}[Data]
in
  TableNavigation;
