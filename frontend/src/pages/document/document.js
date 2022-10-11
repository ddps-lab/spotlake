import React from "react";
import {ContentBox, Box, SubTitle, Title, Wrapper} from "../../UI/styles";
import styled from "styled-components";

function Document () {
    return (
        <Wrapper padding={`1rem 4rem`}>
            <Box column align={'flex-start'} gap={1} margin={`20px 0`}>
                <DemoImg src={process.env.PUBLIC_URL + "/demoImg.png"} alt={"demoPageImg"}/>
                <SubTitle>(1) Vendor selection</SubTitle>
                <ContentBox>
                    On the demo page, users can select one cloud vendor among AWS, Google Cloud, or Azure to show the latest spot instance dataset in the table below. The table shows the latest dataset of the selected cloud vendor, and it contains every pair of instance types and regions provided by the vendor.
                </ContentBox>
            </Box>
            <Box column align={'flex-start'} gap={1} margin={`20px 0`}>
                <SubTitle>(2) Querying</SubTitle>
                <ContentBox>
                    Since the default table shows only the latest dataset of every instance-region pair, users have to query with specific Instance Type, Region, AZ, and Date Range options to get the historical dataset. Data query has some limitations; the maximum number of the returned data point is 20,000 and user can set the date range up to 1 month. If user selects the ‘ALL’ option in Region or AZ field, the returned dataset contains every Regions or AZs corresponding to the Instance Type option.
                    <br/>
                    <br/>
                    Even if user send query with specific date range, SpotLake does not return data points in the date range. SpotLake system only saves the data point when there is a change in any fields. Therefore, user only get the changed data points with demo page’s querying feature. If you want to get the full dataset, check the ‘How to access full dataset’ section on about page.
                </ContentBox>
            </Box>
            <Box column align={'flex-start'} gap={1} margin={`20px 0`}>
                <SubTitle>(3) Filtering</SubTitle>
                <ContentBox>
                    User can apply additional filter to the table that shows default latest dataset or queried dataset. For instance, user can select specific data points that contains specific character in Instance Type column or filter by size of the score. Also table could be exported in the CSV format with EXPORT button.
                </ContentBox>
            </Box>
        </Wrapper>
    )
}
const DemoImg = styled.img`
    max-width: 80%;
    max-height: 500px;
    margin: 0 auto 20px auto;
`;
export default Document;