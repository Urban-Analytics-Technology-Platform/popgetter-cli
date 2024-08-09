use comfy_table::{presets::NOTHING, *};
use itertools::izip;
use popgetter::search::SearchResults;

pub fn display_search_results(results: SearchResults, max_results: Option<usize>) {
    let df_to_show = match max_results {
        Some(max) => results.0.head(Some(max)),
        None => results.0,
    };

    for (metric_id, hrn, desc, hxl, level) in izip!(
        df_to_show.column("metric_id").unwrap().iter(),
        df_to_show.column("human_readable_name").unwrap().iter(),
        df_to_show.column("metric_description").unwrap().iter(),
        df_to_show.column("metric_hxl_tag").unwrap().iter(),
        df_to_show.column("geometry_level").unwrap().iter(),
    ) {
        let mut table = Table::new();
        table
            .load_preset(NOTHING)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_style(comfy_table::TableComponent::BottomBorder, '─')
            .set_style(comfy_table::TableComponent::BottomBorderIntersections, '─')
            .set_style(comfy_table::TableComponent::TopBorder, '─')
            .set_style(comfy_table::TableComponent::TopBorderIntersections, '─')
            .add_row(vec![
                Cell::new("Metric ID").add_attribute(Attribute::Bold),
                metric_id.get_str().unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Human readable name").add_attribute(Attribute::Bold),
                hrn.get_str().unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Description").add_attribute(Attribute::Bold),
                desc.get_str().unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("HXL tag").add_attribute(Attribute::Bold),
                hxl.get_str().unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Geometry level").add_attribute(Attribute::Bold),
                level.get_str().unwrap().into(),
            ]);

        let column = table.column_mut(0).unwrap();
        column.set_cell_alignment(CellAlignment::Right);

        println!("\n{}", table);
    }
}
