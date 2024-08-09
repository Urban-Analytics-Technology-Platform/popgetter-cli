use comfy_table::{presets::NOTHING, *};
use itertools::izip;

use polars::{frame::DataFrame, prelude::SortMultipleOptions};
use popgetter::{search::SearchResults, COL};

pub fn display_countries(countries: DataFrame, max_results: Option<usize>) -> anyhow::Result<()> {
    let df_to_show = match max_results {
        Some(max) => countries.head(Some(max)),
        None => countries,
    };
    let df_to_show = df_to_show.sort([COL::COUNTRY_ID], SortMultipleOptions::default())?;
    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Country ID").add_attribute(Attribute::Bold),
            Cell::new("Country Name (official)").add_attribute(Attribute::Bold),
            Cell::new("Country Name (short)").add_attribute(Attribute::Bold),
            Cell::new("ISO3116-1 alpha-3").add_attribute(Attribute::Bold),
            Cell::new("ISO3116-2").add_attribute(Attribute::Bold),
        ])
        .set_style(comfy_table::TableComponent::BottomBorder, '─')
        .set_style(comfy_table::TableComponent::MiddleHeaderIntersections, '─')
        .set_style(comfy_table::TableComponent::HeaderLines, '─')
        .set_style(comfy_table::TableComponent::BottomBorderIntersections, '─')
        .set_style(comfy_table::TableComponent::TopBorder, '─')
        .set_style(comfy_table::TableComponent::TopBorderIntersections, '─');
    for (
        country_id,
        country_name_official,
        country_name_short_en,
        countr_iso3,
        country_iso3116_2,
    ) in izip!(
        df_to_show.column(COL::COUNTRY_ID)?.str()?,
        df_to_show.column(COL::COUNTRY_NAME_OFFICIAL)?.str()?,
        df_to_show.column(COL::COUNTRY_NAME_SHORT_EN)?.str()?,
        df_to_show.column(COL::COUNTRY_ISO3)?.str()?,
        df_to_show.column(COL::COUNTRY_ISO3166_2)?.str()?,
    ) {
        table.add_row(vec![
            country_id.unwrap_or_default(),
            country_name_official.unwrap_or_default(),
            country_name_short_en.unwrap_or_default(),
            countr_iso3.unwrap_or_default(),
            country_iso3116_2.unwrap_or_default(),
        ]);
    }
    println!("\n{}", table);
    Ok(())
}

pub fn display_search_results(
    results: SearchResults,
    max_results: Option<usize>,
) -> anyhow::Result<()> {
    let df_to_show = match max_results {
        Some(max) => results.0.head(Some(max)),
        None => results.0,
    };

    for (metric_id, hrn, desc, hxl, date, country, level, download_url) in izip!(
        df_to_show.column(COL::METRIC_ID)?.str()?,
        df_to_show.column(COL::METRIC_HUMAN_READABLE_NAME)?.str()?,
        df_to_show.column(COL::METRIC_DESCRIPTION)?.str()?,
        df_to_show.column(COL::METRIC_HXL_TAG)?.str()?,
        df_to_show
            .column(COL::SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START)?
            .rechunk()
            .iter(),
        df_to_show.column(COL::COUNTRY_NAME_SHORT_EN)?.str()?,
        // Note: if using iter on an AnyValue, need to rechunk first.
        df_to_show.column(COL::GEOMETRY_LEVEL)?.rechunk().iter(),
        df_to_show
            .column(COL::METRIC_SOURCE_DOWNLOAD_URL)?
            .rechunk()
            .iter()
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
                metric_id.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Metric ID (short)").add_attribute(Attribute::Bold),
                metric_id
                    .unwrap()
                    .chars()
                    .take(8)
                    .collect::<String>()
                    .into(),
            ])
            .add_row(vec![
                Cell::new("Human readable name").add_attribute(Attribute::Bold),
                hrn.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Description").add_attribute(Attribute::Bold),
                desc.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("HXL tag").add_attribute(Attribute::Bold),
                hxl.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Collection date").add_attribute(Attribute::Bold),
                format!("{date}").into(),
            ])
            .add_row(vec![
                Cell::new("Country").add_attribute(Attribute::Bold),
                country.unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Geometry level").add_attribute(Attribute::Bold),
                level.get_str().unwrap().into(),
            ])
            .add_row(vec![
                Cell::new("Source download URL").add_attribute(Attribute::Bold),
                download_url.get_str().unwrap().into(),
            ]);

        let column = table.column_mut(0).unwrap();
        column.set_cell_alignment(CellAlignment::Right);

        println!("\n{}", table);
    }
    Ok(())
}
