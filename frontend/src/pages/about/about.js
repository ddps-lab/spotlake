import React from "react";
import {ContentBox, Box, SubTitle, Wrapper, ButtonICon, theme, CustomButton} from "../../UI/styles";
import { ThemeProvider } from '@mui/material/styles';
function About () {
    return (
        <Wrapper>
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
                    <ul className="buttonGroup">
                        <li>
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    color={"IISWC"}
                                    variant="contained"
                                    onClick={() => {alert("link will be available soon")}}
                                >IISWC 2022 paper</CustomButton>
                            </ThemeProvider>
                        </li>
                        <li style={{marginTop : "20px"}}>
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    startIcon={<ButtonICon size={"24px"} src={process.env.PUBLIC_URL + "/icon/arxiv-logo-1.png"} />}
                                    color={"arxiv"}
                                    variant="contained"
                                    onClick={() => {window.open('https://arxiv.org/abs/2202.02973')}}
                                >arXiv paper</CustomButton>
                            </ThemeProvider>
                        </li>
                    </ul>
                    Every source code and the issue of the SpotLake system is maintained through the GitHub repository. Anyone interested in the SpotLake system could contribute to the code. You can check the star button if you are intriguing this open-source project.
                    <ul className="buttonGroup">
                        <li>
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    size={"small"}
                                    startIcon={<ButtonICon src={process.env.PUBLIC_URL + "/icon/GitHub-Mark-120px-plus.png"} />}
                                    color={"github"}
                                    variant="contained"
                                    onClick={() => {window.open('https://github.com/ddps-lab')}}
                                    >Github</CustomButton>
                            </ThemeProvider>
                        </li>
                        <li style={{marginTop : "20px"}}>
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    size={"small"}
                                    startIcon={<ButtonICon src={process.env.PUBLIC_URL + "/icon/GitHub-Mark-120px-plus.png"} />}
                                    color={"github"}
                                    variant="contained"
                                    onClick={() => {window.open('https://github.com/ddps-lab/spotlake')}}
                                >Star</CustomButton>
                            </ThemeProvider>
                        </li>
                    </ul>
                </ContentBox>
            </Box>
            <Box column align={'flex-start'} gap={1} margin={'20px 0'}>
                <SubTitle>How to access full dataset</SubTitle>
                <ContentBox className="about">
                    We can not provide the full dataset through this web-service because the dataset is too large. Those who want to access the full dataset of the SpotLake system, please fill out the google form below and we will give you access permission for the full dataset.
                    <ul className="buttonGroup">
                        <ThemeProvider theme={theme}>
                            <CustomButton
                                startIcon={<ButtonICon src={"https://upload.wikimedia.org/wikipedia/commons/thumb/5/5b/Google_Forms_2020_Logo.svg/1489px-Google_Forms_2020_Logo.svg.png"} />}
                                color={"GoogleForm"}
                                variant="contained"
                                onClick={() => {window.open('https://forms.gle/FKqFVYUAJgGH34nJ7')}}
                            >Google Form</CustomButton>
                        </ThemeProvider>
                    </ul>
                </ContentBox>
            </Box>
        </Wrapper>
    )
}
export default About;