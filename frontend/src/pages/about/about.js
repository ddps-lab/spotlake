import React from "react";
import {ContentBox, Box, SubTitle, Title, Wrapper} from "../../UI/styles";

function About () {
    return (
        <Wrapper padding={`1rem 4rem`}>
            <Box column align={'flex-start'} gap={1} margin={`20px 0`}>
                <SubTitle>What is SpotLake system?</SubTitle>
                <ContentBox>
                    SpotLake system is an integrated data archive service that provides spot instance datasets collected from diverse public cloud vendors.The datasets include various information about spot instances like spot availability, spot interruption frequency, and spot price. Researchers and developers can utilize the SpotLake system to make their own system more cost-efficiently. SpotLake system currently provides the latest and restricted range of spot datasets collected from AWS, Google Cloud, and Azure through a demo page. We believe numerous systems could achieve a huge improvement in cost efficiency by utilizing the SpotLake system.
                </ContentBox>
            </Box>
            <Box column align={'flex-start'} gap={1} margin={'20px 0'}>
                <SubTitle>Paper and code</SubTitle>
                <ContentBox className="about">
                    If you are interested in an analysis of the SpotLake datasets or system implementation, check the latest version of the SpotLake paper which is published in IISWC 2022. We also published an older version of the paper through arXiv.
                    <ul>
                        <li>IISWC 2022 paper (link will be available soon)</li>
                        <li>arXiv paper (<a href="https://arxiv.org/abs/2202.02973">https://arxiv.org/abs/2202.02973</a>)</li>
                    </ul>
                    Every source code and the issue of the SpotLake system is maintained through the GitHub repository. Anyone interested in the SpotLake system could contribute to the code. You can check the star button if you are intriguing this open-source project.
                    <ul>
                        <li>GitHub Link (<a href="https://github.com/ddps-lab/spotlake">https://github.com/ddps-lab/spotlake</a>)</li>
                        <li>GitHub Star (star 버튼 만드는 법 참고: <a href="https://buttons.github.io">https://buttons.github.io</a>)</li>
                    </ul>
                </ContentBox>
            </Box>
            <Box column align={'flex-start'} gap={1} margin={'20px 0'}>
                <SubTitle>How to access full dataset</SubTitle>
                <ContentBox className="about">
                    We can not provide the full dataset through this web-service because the dataset is too large. Those who want to access the full dataset of the SpotLake system, please fill out the google form below and we will give you access permission for the full dataset.
                    <ul>
                        <li>Google Form Link (<a color={"#3d56b2"} href="link">link</a>)</li>
                    </ul>
                </ContentBox>
            </Box>
        </Wrapper>
    )
}
export default About;